"""Tests for the rate-limit rejection collector: axis mapping, identity
parsing, burst folding, and the buffer-overflow path.

No network, no real database — the write path is observed through a patched
``ResourceEvent.create`` and a mocked session, mirroring the surrounding
collector tests."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import gpustack.server.limit_event_collector as lec
from gpustack.schemas.resource_events import (
    EVENT_TYPE_LIMIT_CONCURRENCY,
    EVENT_TYPE_LIMIT_EVENT,
    EVENT_TYPE_LIMIT_QUOTA_DAILY,
    EVENT_TYPE_LIMIT_QUOTA_MONTHLY,
    EVENT_TYPE_LIMIT_QUOTA_WEEKLY,
    EVENT_TYPE_LIMIT_QUOTA_YEARLY,
    EVENT_TYPE_LIMIT_RATE_REQUESTS,
    EVENT_TYPE_LIMIT_RATE_TOKENS,
    RESOURCE_TYPE_LIMIT,
)
from gpustack.server.limit_event_collector import (
    GatewayLimitEvent,
    accumulate_limit_events,
    flush_limit_events,
    limit_event_message,
    limit_event_type,
    parse_consumer_user_id,
)


@pytest.fixture(autouse=True)
def _clean_buffers():
    lec._pending_events.clear()
    lec._overflow_counts.clear()
    yield
    lec._pending_events.clear()
    lec._overflow_counts.clear()


@pytest.mark.parametrize(
    "consumer,expected",
    [
        ("gpustack-3970", 3970),
        ("ak-abc123.gpustack-42", 42),
        (None, None),
        ("", None),
        ("gpustack-notanumber", None),
        ("someone-else", None),
    ],
)
def test_parse_consumer_user_id(consumer, expected):
    assert parse_consumer_user_id(consumer) == expected


@pytest.mark.parametrize(
    "kind,period,expected",
    [
        ("query", "60s", EVENT_TYPE_LIMIT_RATE_REQUESTS),
        ("query", "86400s", EVENT_TYPE_LIMIT_RATE_REQUESTS),
        ("token_rolling", "60s", EVENT_TYPE_LIMIT_RATE_TOKENS),
        # The weekly budget rides the token_rolling kind (sliding 7-day
        # window), but gets its own event type so the usage views can tell it
        # apart from the per-minute TPM axis.
        ("token_rolling", "604800s", EVENT_TYPE_LIMIT_QUOTA_WEEKLY),
        ("token_calendar", "each_day", EVENT_TYPE_LIMIT_QUOTA_DAILY),
        ("token_calendar", "each_month", EVENT_TYPE_LIMIT_QUOTA_MONTHLY),
        ("token_calendar", "each_year", EVENT_TYPE_LIMIT_QUOTA_YEARLY),
        # An unknown calendar period stays attributable but generic.
        ("token_calendar", "each_week", EVENT_TYPE_LIMIT_EVENT),
        ("concurrency", "7200s", EVENT_TYPE_LIMIT_CONCURRENCY),
        ("mystery", "60s", EVENT_TYPE_LIMIT_EVENT),
        (None, None, EVENT_TYPE_LIMIT_EVENT),
    ],
)
def test_limit_event_type(kind, period, expected):
    assert limit_event_type(kind, period) == expected


@pytest.mark.parametrize(
    "kind,period,expected",
    [
        ("query", "60s", "Too many requests per minute"),
        ("query", "90s", "Too many requests per window"),
        ("token_rolling", "60s", "Too many tokens per minute"),
        ("token_rolling", "604800s", "Weekly token quota exceeded"),
        ("token_calendar", "each_month", "Monthly token quota exceeded"),
        ("token_calendar", "each_day", "Daily token quota exceeded"),
        ("token_calendar", "each_year", "Yearly token quota exceeded"),
        ("token_calendar", "each_week", "Token quota exceeded"),
        ("concurrency", "7200s", "Too many concurrent requests for this model"),
        ("mystery", "60s", "Rate limit exceeded"),
    ],
)
def test_limit_event_message(kind, period, expected):
    assert limit_event_message(kind, period) == expected


@contextmanager
def _patch_write(users: dict):
    """Patch the user resolution and the row write, yielding the create mock."""
    with (
        patch.object(lec, "UserService") as US,
        patch.object(lec.ResourceEvent, "create", new=AsyncMock()) as create,
        patch.object(lec, "async_session") as session_factory,
    ):
        US.return_value.get_by_id = AsyncMock(side_effect=lambda uid: users.get(uid))
        session_ctx = session_factory.return_value
        session_ctx.__aenter__ = AsyncMock(return_value=session_ctx)
        session_ctx.__aexit__ = AsyncMock(return_value=False)
        session_ctx.commit = AsyncMock()
        yield create, session_ctx


def _event(uid=3970, kind="token_calendar", period="each_month", **kw):
    return GatewayLimitEvent(
        occurred_at=datetime(2026, 11, 24, 7, 40, 31, tzinfo=timezone.utc),
        consumer=f"gpustack-{uid}",
        combo=f"user-{uid}",
        kind=kind,
        period=period,
        **kw,
    )


@pytest.mark.asyncio
async def test_flush_writes_resource_event_row():
    users = {3970: SimpleNamespace(id=3970, name="denis.petrov_ext@lemanapro.ru")}
    with _patch_write(users) as (create, session):
        await accumulate_limit_events([_event(model="qwen3-32b")])
        await flush_limit_events()

    assert create.await_count == 1
    row = create.await_args.args[1]
    assert row.resource_type == RESOURCE_TYPE_LIMIT
    assert row.resource_id == 3970
    assert row.resource_name == "denis.petrov_ext@lemanapro.ru"
    assert row.creator_id == 3970
    assert row.creator_name == "denis.petrov_ext@lemanapro.ru"
    assert row.consumer_principal_id == 3970
    assert row.event_type == EVENT_TYPE_LIMIT_QUOTA_MONTHLY
    assert row.event_message == "Monthly token quota exceeded, model=qwen3-32b"
    # A plain row stands for exactly one rejection.
    assert row.event_count == 1
    # Naive UTC — the events table anchors on the storage convention.
    assert row.occurred_at == datetime(2026, 11, 24, 7, 40, 31)
    assert row.spec_snapshot["combo"] == "user-3970"
    assert row.spec_snapshot["count"] == 1
    # One commit per flush, not per row.
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush_folds_burst_beyond_row_cap():
    users = {3970: SimpleNamespace(id=3970, name="denis.petrov_ext@lemanapro.ru")}
    base = datetime(2026, 11, 24, 7, 40, 0, tzinfo=timezone.utc)
    events = [
        GatewayLimitEvent(
            occurred_at=base + timedelta(seconds=i),
            consumer="gpustack-3970",
            combo="user-3970",
            kind="query",
            period="60s",
            model="qwen3-32b",
        )
        for i in range(50)
    ]
    with _patch_write(users) as (create, _session):
        await accumulate_limit_events(events)
        await flush_limit_events()

    # Five exact rows carry the burst; the tail is summed into the last one.
    assert create.await_count == 5
    rows = [call.args[1] for call in create.await_args_list]
    assert [r.event_message for r in rows[:4]] == [
        "Too many requests per minute, model=qwen3-32b"
    ] * 4
    assert rows[4].event_message == (
        "Too many requests per minute, +45 suppressed, model=qwen3-32b"
    )
    # Timestamps stay exact and ordered for the kept rows.
    assert rows[0].occurred_at == base.replace(tzinfo=None)
    assert rows[4].occurred_at == (base + timedelta(seconds=4)).replace(tzinfo=None)
    assert rows[4].spec_snapshot["count"] == 46
    # The counts are first-class: plain rows carry 1, the folded tail row
    # carries the burst, so the usage views can sum exact rejections.
    assert [r.event_count for r in rows] == [1, 1, 1, 1, 46]


@pytest.mark.asyncio
async def test_flush_keeps_distinct_models_separate():
    users = {3970: SimpleNamespace(id=3970, name="denis.petrov_ext@lemanapro.ru")}
    events = [
        _event(model="qwen3-32b"),
        _event(model="glm-5.3-flash"),
    ]
    with _patch_write(users) as (create, _session):
        await accumulate_limit_events(events)
        await flush_limit_events()

    assert create.await_count == 2
    models = {call.args[1].spec_snapshot["model"] for call in create.await_args_list}
    assert models == {"qwen3-32b", "glm-5.3-flash"}


@pytest.mark.asyncio
async def test_buffer_overflow_degrades_to_one_row_per_key():
    users = {3970: SimpleNamespace(id=3970, name="denis.petrov_ext@lemanapro.ru")}
    with patch.object(lec.envs, "LIMIT_EVENTS_BUFFER_MAX_SIZE", 3):
        with _patch_write(users) as (create, _session):
            events = [
                GatewayLimitEvent(
                    occurred_at=datetime(2026, 11, 24, 8, 0, i, tzinfo=timezone.utc),
                    consumer="gpustack-3970",
                    combo="user-3970",
                    kind="query",
                    period="60s",
                    model="qwen3-32b",
                )
                for i in range(10)
            ]
            await accumulate_limit_events(events)
            await flush_limit_events()

    # The first three filled the buffer as exact rows; the remaining seven
    # collapsed into one attributable overflow row.
    assert create.await_count == 4
    rows = [call.args[1] for call in create.await_args_list]
    overflow = rows[-1]
    assert overflow.event_type == EVENT_TYPE_LIMIT_RATE_REQUESTS
    assert overflow.resource_id == 3970
    assert overflow.event_message == (
        "Rate limit events buffer overflow, 7 events suppressed"
    )
    assert overflow.spec_snapshot["count"] == 7
    assert overflow.event_count == 7


@pytest.mark.asyncio
async def test_unresolvable_user_keeps_the_raw_consumer():
    with _patch_write({}) as (create, _session):
        await accumulate_limit_events([_event(uid=404)])
        await flush_limit_events()

    row = create.await_args.args[1]
    assert row.creator_id == 404
    assert row.creator_name is None
    assert row.resource_name == "gpustack-404"
    assert row.consumer_principal_id is None
