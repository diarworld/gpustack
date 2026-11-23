"""The group-quota resolution: priority, per-field min, and the loader.

The pure resolver matrix covers the priority the feature ships with
(user explicit > smallest group value > deployment default, per field
independently) plus the per-model concurrency edge cases. The loader runs
against a real in-memory SQLite so the transitive closure and the
Org-exclusion hold against actual rows.
"""

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.config.config import GatewayRateLimitConfig
from gpustack.gateway.effective_limits import (
    EffectiveLimitsView,
    effective_limits_view,
    load_group_limit_snapshots,
    resolve_effective_limits,
)
from gpustack.schemas.principals import (
    Principal,
    PrincipalMembership,
    PrincipalType,
)


def _rlc(**overrides) -> GatewayRateLimitConfig:
    fields = dict(
        enabled=True,
        default_rpm=100,
        default_tpm=200_000,
        default_monthly_tokens=10_000_000,
        default_concurrency=3,
    )
    fields.update(overrides)
    return GatewayRateLimitConfig(**fields)


def _user(**limits):
    """A duck-typed Principal row: only the five limit fields are read."""
    return SimpleNamespaceUser(
        rpm=limits.get("rpm"),
        tpm=limits.get("tpm"),
        weekly=limits.get("weekly"),
        monthly=limits.get("monthly"),
        concurrency=limits.get("concurrency"),
        is_admin=limits.get("is_admin", False),
    )


class SimpleNamespaceUser:
    def __init__(self, rpm, tpm, weekly, monthly, concurrency, is_admin):
        self.id = 42
        self.is_admin = is_admin
        self.rpm_limit = rpm
        self.tpm_limit = tpm
        self.weekly_token_limit = weekly
        self.monthly_token_limit = monthly
        self.model_concurrency_limits = concurrency


def _group(**limits):
    return SimpleNamespaceUser(
        rpm=limits.get("rpm"),
        tpm=limits.get("tpm"),
        weekly=limits.get("weekly"),
        monthly=limits.get("monthly"),
        concurrency=limits.get("concurrency"),
        is_admin=False,
    )


class TestScalarResolution:
    def test_user_explicit_wins_in_both_directions(self):
        # Tighter and looser than the groups alike: an explicit user
        # setting is the whole point of the first priority level.
        user = _user(rpm=5)
        loosish = [_group(rpm=2), _group(rpm=500)]
        assert resolve_effective_limits(user, loosish, _rlc()).rpm_limit == 5
        user = _user(rpm=1000)
        assert resolve_effective_limits(user, loosish, _rlc()).rpm_limit == 1000

    def test_min_across_groups_per_field_independently(self):
        # The doc example: each field resolves on its own, so the user
        # lands on the strictest value of each — RPM 2 from group2,
        # TPM 1000 from group1.
        user = _user()
        groups = [
            _group(rpm=10, tpm=1000),
            _group(rpm=2, tpm=10_000),
        ]
        resolved = resolve_effective_limits(user, groups, _rlc())
        assert resolved.rpm_limit == 2
        assert resolved.tpm_limit == 1000

    def test_groups_relax_the_deployment_default(self):
        # Group priority over the default is what makes "this team gets
        # more RPM than everyone else" work: 500 beats the default 100.
        resolved = resolve_effective_limits(_user(), [_group(rpm=500)], _rlc())
        assert resolved.rpm_limit == 500

    def test_groups_tighten_the_deployment_default(self):
        resolved = resolve_effective_limits(_user(), [_group(rpm=7)], _rlc())
        assert resolved.rpm_limit == 7

    def test_unset_everywhere_falls_back_to_the_default(self):
        resolved = resolve_effective_limits(_user(), [_group()], _rlc())
        assert resolved.rpm_limit == 100
        assert resolved.tpm_limit == 200_000
        assert resolved.monthly_token_limit == 10_000_000
        # The weekly budget has no deployment default by design: a default
        # budget silently capping every account is not a safe implication.
        assert resolved.weekly_token_limit is None

    def test_weekly_comes_only_from_the_user_or_groups(self):
        assert (
            resolve_effective_limits(
                _user(), [_group(weekly=900)], _rlc()
            ).weekly_token_limit
            == 900
        )
        assert (
            resolve_effective_limits(
                _user(weekly=50), [_group(weekly=900)], _rlc()
            ).weekly_token_limit
            == 50
        )

    def test_partial_group_fields_fall_through_per_field(self):
        # A group setting only RPM leaves TPM/monthly to the default.
        resolved = resolve_effective_limits(_user(), [_group(rpm=9)], _rlc())
        assert resolved.rpm_limit == 9
        assert resolved.tpm_limit == 200_000
        assert resolved.monthly_token_limit == 10_000_000

    def test_no_groups_matches_the_legacy_behavior(self):
        # No groups: the user's explicit values over the defaults, exactly
        # as before group quotas existed.
        resolved = resolve_effective_limits(_user(rpm=17), [], _rlc())
        assert resolved.rpm_limit == 17
        resolved = resolve_effective_limits(_user(), [], _rlc())
        assert resolved.rpm_limit == 100


class TestConcurrencyResolution:
    def test_user_entry_wins_even_zero(self):
        # A stored 0 on the user is the explicit "unlimited" escape hatch;
        # a group cap must not resurrect a limit under it.
        user = _user(concurrency={"m1": 0})
        groups = [_group(concurrency={"m1": 2})]
        resolved = resolve_effective_limits(user, groups, _rlc())
        assert resolved.model_concurrency_limits == {"m1": 0}

    def test_min_of_nonzero_group_values(self):
        user = _user()
        groups = [
            _group(concurrency={"m1": 5, "m2": 1}),
            _group(concurrency={"m1": 8}),
        ]
        resolved = resolve_effective_limits(user, groups, _rlc())
        assert resolved.model_concurrency_limits == {"m1": 5, "m2": 1}

    def test_group_zero_does_not_constrain(self):
        # A group 0 declares "this group does not constrain the model":
        # it never enters the min, and it can neither tighten the
        # deployment default nor lift a sibling group's cap.
        user = _user()
        groups = [
            _group(concurrency={"m1": 0}),
            _group(concurrency={"m1": 4}),
        ]
        resolved = resolve_effective_limits(user, groups, _rlc())
        assert resolved.model_concurrency_limits == {"m1": 4}

    def test_models_nobody_caps_stay_out_of_the_map(self):
        # The map carries overrides only; models absent from every map
        # ride the deployment default through the combo's block default.
        user = _user(concurrency={"m1": 2})
        groups = [_group(concurrency={"m2": 6})]
        resolved = resolve_effective_limits(user, groups, _rlc())
        assert resolved.model_concurrency_limits == {"m1": 2, "m2": 6}
        assert "m3" not in (resolved.model_concurrency_limits or {})

    def test_nothing_constraining_resolves_to_none(self):
        resolved = resolve_effective_limits(_user(), [_group()], _rlc())
        assert resolved.model_concurrency_limits is None


class TestEffectiveLimitsView:
    def test_view_duck_types_the_limit_builders(self):
        # id and is_admin pass through; the five fields carry resolved
        # values — this is the shape user_limits_combination reads.
        user = _user(rpm=5)
        view = effective_limits_view(user, [_group(rpm=2, tpm=1000)], _rlc())
        assert isinstance(view, EffectiveLimitsView)
        assert view.id == 42
        assert view.is_admin is False
        assert view.rpm_limit == 5
        assert view.tpm_limit == 1000

    def test_admin_flag_passes_through(self):
        view = effective_limits_view(_user(is_admin=True), [], _rlc())
        assert view.is_admin is True


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite://")

    # SQLite's built-in lower() folds ASCII only; the loader's statements
    # do not lean on lower(), but the override keeps the fixture consistent
    # with the other principal-row tests.

    @event.listens_for(engine.sync_engine, "connect")
    def _unicode_lower(dbapi_conn, _record):
        dbapi_conn.create_function(
            "lower", 1, lambda s: s.lower() if isinstance(s, str) else s
        )

    async with engine.begin() as conn:
        await conn.run_sync(Principal.__table__.create)
        await conn.run_sync(PrincipalMembership.__table__.create)
    async with AsyncSession(engine, expire_on_commit=False) as s:
        yield s
    await engine.dispose()


async def _add_group(session, principal_id: int, name: str) -> Principal:
    group = Principal(
        id=principal_id,
        kind=PrincipalType.GROUP,
        name=name,
        rpm_limit=None,
        source="Local",
    )
    session.add(group)
    await session.commit()
    return group


async def _add_user(session, principal_id: int, name: str) -> Principal:
    user = Principal(
        id=principal_id,
        kind=PrincipalType.USER,
        name=name,
        source="Local",
    )
    session.add(user)
    await session.commit()
    return user


async def _add_org(session, principal_id: int, name: str) -> Principal:
    org = Principal(
        id=principal_id,
        kind=PrincipalType.ORG,
        name=name,
        source="Local",
    )
    session.add(org)
    await session.commit()
    return org


async def _edge(session, parent_id: int, member_id: int) -> None:
    session.add(
        PrincipalMembership(
            parent_principal_id=parent_id, member_principal_id=member_id
        )
    )
    await session.commit()


class TestLoadGroupLimitSnapshots:
    @pytest.mark.asyncio
    async def test_direct_and_transitive_groups(self, session):
        await _add_user(session, 5, "u5@example.com")
        await _add_group(session, 10, "team")
        await _add_group(session, 20, "title/разработчик")
        await _add_group(session, 30, "dept/713")
        await _edge(session, 10, 5)  # u5 in team
        await _edge(session, 20, 10)  # team in title/разработчик
        await _edge(session, 30, 20)  # title/разработчик in dept/713

        snapshots = await load_group_limit_snapshots(session, [5])
        assert sorted(g.id for g in snapshots[5]) == [10, 20, 30]

    @pytest.mark.asyncio
    async def test_org_membership_never_contributes(self, session):
        # A group's Org membership confers roles, not quotas: an ORG
        # parent must not appear in the closure.
        await _add_user(session, 5, "u5@example.com")
        await _add_group(session, 10, "team")
        await _add_org(session, 40, "org")
        await _edge(session, 10, 5)
        await _edge(session, 40, 10)

        snapshots = await load_group_limit_snapshots(session, [5])
        assert [g.id for g in snapshots[5]] == [10]

    @pytest.mark.asyncio
    async def test_batch_resolution_and_missing_users(self, session):
        await _add_user(session, 5, "u5@example.com")
        await _add_user(session, 6, "u6@example.com")
        await _add_group(session, 10, "team")
        await _edge(session, 10, 5)

        snapshots = await load_group_limit_snapshots(session, [5, 6])
        assert [g.id for g in snapshots[5]] == [10]
        assert snapshots[6] == []

    @pytest.mark.asyncio
    async def test_empty_request(self, session):
        assert await load_group_limit_snapshots(session, []) == {}
