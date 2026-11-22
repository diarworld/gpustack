"""The route counters are recomputed, not remembered.

`ModelRoute.ready_targets` is a denormalized count that `/v1/models` filters
on (`ready_targets > 0`), maintained by an edge-triggered chain: a target
flips, the flip is published, the route controller re-counts on the published
event. In distributed mode the event travels id-only and is rehydrated on
arrival, so any hop of that chain can be lost without a trace — and the
counter then disagrees with the target rows for as long as nothing else
touches the route: a model that is not running yet stays listed, and one that
recovered stays hidden. These pin the level-triggered repairs: the recount
from target rows, the recount that rides the same pass that flips a target,
and the notification that stamps fresh counters on what it publishes.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.server.bus import Event, EventType, event_bus
from gpustack.server.controllers import (
    ModelRouteTargetController,
    reconcile_route_counters,
    reconcile_route_target_states,
)
from gpustack.schemas.model_routes import (
    ModelRoute,
    ModelRouteTarget,
    TargetStateEnum,
)
from gpustack.schemas.models import Model, ModelStateEnum
from gpustack.schemas.principals import Principal, PrincipalType
from tests.utils.model import new_model


@pytest_asyncio.fixture
async def engine():
    e = create_async_engine("sqlite+aiosqlite://")
    async with e.begin() as conn:
        for table in (
            Principal.__table__,
            Model.__table__,
            ModelRoute.__table__,
            ModelRouteTarget.__table__,
        ):
            await conn.run_sync(table.create)
    yield e
    await e.dispose()


@pytest.fixture
def session_factory(engine, monkeypatch):
    factory = lambda: AsyncSession(engine, expire_on_commit=False)  # noqa: E731
    monkeypatch.setattr("gpustack.server.controllers.async_session", factory)
    return factory


@pytest.fixture
def published(monkeypatch):
    """Capture every event publish: writes on these paths announce themselves
    through the bus, and a real publish would spawn stray tasks the test loop
    never drains."""
    events = []

    async def record(topic, event):
        events.append((topic, event))

    monkeypatch.setattr(event_bus, "publish", record)
    return events


async def _seed(engine, *, model_state=ModelStateEnum.RUNNING):
    """The incident's shape: a servable model, two targets still UNAVAILABLE
    (their repair event was lost), and a route row whose ready_targets=1 was
    written twenty hours ago."""
    async with AsyncSession(engine, expire_on_commit=False) as s:
        s.add(Principal(id=1, kind=PrincipalType.ORG, name="acme"))
        model = new_model(1, "demo", huggingface_repo_id="org/demo")
        model.state = model_state
        s.add(model)
        s.add(
            ModelRoute(
                id=1,
                name="demo",
                created_model_id=1,
                targets=2,
                ready_targets=1,
                owner_principal_id=1,
            )
        )
        for name in ("t1", "t2"):
            s.add(
                ModelRouteTarget(
                    id=1 if name == "t1" else 2,
                    name=name,
                    route_name="demo",
                    route_id=1,
                    model_id=1,
                    state=TargetStateEnum.UNAVAILABLE,
                )
            )
        await s.commit()


async def _route(engine):
    async with AsyncSession(engine, expire_on_commit=False) as s:
        return await ModelRoute.one_by_id(s, 1)


# --- the recount ------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_recount_repairs_the_drifted_route(engine, session_factory, published):
    await _seed(engine)

    async with session_factory() as session:
        assert await reconcile_route_counters(session, 1) is True

    route = await _route(engine)
    assert (route.targets, route.ready_targets) == (2, 0)

    # Idempotent: a fleet that is already consistent writes nothing.
    async with session_factory() as session:
        assert await reconcile_route_counters(session, 1) is False


@pytest.mark.asyncio
async def test_recount_deletes_a_route_that_lost_its_targets(
    engine, session_factory, published
):
    await _seed(engine)
    async with AsyncSession(engine, expire_on_commit=False) as s:
        for t in await ModelRouteTarget.all(s):
            t.deleted_at = datetime.now(timezone.utc)
            await t.update(session=s, auto_commit=False)
        await s.commit()

    async with session_factory() as session:
        assert await reconcile_route_counters(session, 1) is True

    assert await _route(engine) is None


# --- the fast path: the flip re-counts its own route -------------------------- #


@pytest.mark.asyncio
async def test_repairing_a_target_recounts_its_route_in_the_same_pass(
    engine, session_factory, published
):
    await _seed(engine)

    async with session_factory() as session:
        model = await session.get(Model, 1)
        assert await reconcile_route_target_states(session, model) is True

    # One pass leaves both the targets and the counters consistent.
    route = await _route(engine)
    assert (route.targets, route.ready_targets) == (2, 2)
    async with session_factory() as session:
        states = {t.name: t.state for t in await ModelRouteTarget.all(session)}
    assert set(states.values()) == {TargetStateEnum.ACTIVE}


# --- the notification stamps what it publishes -------------------------------- #


@pytest.mark.asyncio
async def test_notify_parents_stamps_fresh_counters_not_the_stale_row(
    engine, session_factory, published
):
    await _seed(engine)
    # The route row is stale; one target has since recovered in the DB.
    async with session_factory() as s:
        target = await ModelRouteTarget.one_by_id(s, 1)
        target.state = TargetStateEnum.ACTIVE
        await target.update(session=s, auto_commit=True)

    # The event exactly as the bus rehydrates it: full row, changed_fields
    # rebuilt by diffing the re-read against the change-detector cache.
    event = Event(
        type=EventType.UPDATED,
        data=target,
        changed_fields={"state": (TargetStateEnum.UNAVAILABLE, TargetStateEnum.ACTIVE)},
        id=1,
    )

    controller = ModelRouteTargetController(SimpleNamespace())
    async with session_factory() as session:
        await controller._notify_parents(session, target, event)

    route_events = [e for topic, e in published if topic == "modelroute"]
    assert len(route_events) == 1
    stamped = route_events[0].data
    # One of two targets is ACTIVE — not the row's stale 0, and not the
    # pre-reconcile value a snapshot of the row would have carried.
    assert (stamped.targets, stamped.ready_targets) == (2, 1)


# --- the durability net -------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_periodic_pass_repairs_states_and_counters(
    engine, session_factory, published
):
    await _seed(engine)
    controller = ModelRouteTargetController(SimpleNamespace())

    await controller._resync_all_targets()
    await controller._resync_all_route_counters()

    route = await _route(engine)
    assert (route.targets, route.ready_targets) == (2, 2)
