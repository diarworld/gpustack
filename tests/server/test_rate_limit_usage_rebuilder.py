"""Tests for the startup rebuild of the gateway's Redis rate-limit counters.

The rebuilder must reproduce the plugin's own accounting closely enough to
restore lost quotas: the right keys (user-level buckets on
``gpustack-<id>``, key buckets on the full consumer identity), the right
token totals per hour bucket, a startup warm-up that replaces whatever
survived, and a hard rule that steady-state cycles never touch live
counters. Resets (``gateway_rate_limit_resets``) forgive earlier spend, so
a rebuilt weekly / monthly counter resumes from the reset.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from gpustack.config.config import GatewayRateLimitConfig, GatewayRateLimitRedis
from gpustack.schemas.api_keys import ApiKey
from gpustack.schemas.principals import Principal
from gpustack.server import rate_limit_usage_rebuilder as rebuilder
from gpustack.gateway.rate_limit_usage import (
    key_windows,
    user_limit_windows,
    default_plan_windows,
)

FIXED_NOW = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)


class FrozenDateTime(rebuilder.datetime):
    @classmethod
    def now(cls, tz=None):
        return FIXED_NOW if tz is None else FIXED_NOW.astimezone(tz)


def _cfg(**overrides) -> GatewayRateLimitConfig:
    fields = dict(
        enabled=True,
        redis=GatewayRateLimitRedis(service_name="rate-limit-redis", password="pw"),
        timezone="UTC",
        default_rpm=10,
        default_tpm=60_000,
        default_daily_tokens=2_000_000,
        default_monthly_tokens=20_000_000,
    )
    fields.update(overrides)
    return GatewayRateLimitConfig(**fields)


def _user(id: int, *, weekly=None, monthly=None) -> Principal:
    return Principal(
        id=id,
        name=f"user-{id}",
        rpm_limit=None,
        tpm_limit=None,
        weekly_token_limit=weekly,
        monthly_token_limit=monthly,
    )


def _key(id: int, *, access_key="ak", user_id=7, daily_token_limit=None) -> ApiKey:
    return ApiKey(
        id=id,
        name=f"key-{id}",
        user_id=user_id,
        access_key=access_key,
        hashed_secret_key=f"hash-{id}",
        daily_token_limit=daily_token_limit,
    )


def _reset(user_id: int, kind: str, at: datetime):
    """A recorded reset, aware UTC as UTCDateTime returns it."""
    return SimpleNamespace(user_id=user_id, kind=kind, reset_at=at)


# (user_id, api_key_id, completed_at, prompt, completion) — the tuple the
# details scan projects. Timestamps are naive UTC, as the table stores them.
def _row(user_id, api_key_id, ts, prompt, completion):
    return (user_id, api_key_id, ts, prompt, completion)


def hour_ts(naive: datetime) -> int:
    hour = naive.replace(minute=0, second=0, microsecond=0)
    return int(hour.replace(tzinfo=timezone.utc).timestamp())


class FakeResult:
    def __init__(self, items):
        self._items = items

    def all(self):
        return self._items


class FakeAsyncSession:
    """Hands out queued result sets in exec order: users, keys, resets,
    usage rows. A second rebuild cycle re-reads users and keys, so the
    queue serves cyclically from the start once exhausted."""

    def __init__(self, queue):
        self._queue = queue
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def exec(self, statement):
        item = self._queue[self.calls % len(self._queue)]
        self.calls += 1
        return FakeResult(item)


class FakePipeline:
    def __init__(self, client):
        self._client = client
        self._commands = []

    def exists(self, key):
        self._commands.append(("exists", key))
        return self

    def zadd(self, key, mapping):
        self._commands.append(("zadd", key, mapping))
        return self

    def delete(self, key):
        self._commands.append(("delete", key))
        return self

    async def execute(self, raise_on_error=True):
        results = []
        for command in self._commands:
            if command[0] == "exists":
                results.append(1 if command[1] in self._client.existing else 0)
            elif command[0] == "delete":
                self._client.zsets.pop(command[1], None)
                self._client.existing.discard(command[1])
                results.append(1)
            else:
                key, mapping = command[1], command[2]
                self._client.zsets.setdefault(key, {}).update(mapping)
                results.append(len(mapping))
        self._commands = []
        return results


class FakeRedis:
    def __init__(self, existing=None):
        self.existing = set(existing or [])
        self.zsets = {}

    async def exists(self, key):
        return 1 if key in self.existing else 0

    def pipeline(self, transaction=False):
        return FakePipeline(self)


def _tokens(zset: dict) -> dict:
    """member -> (tokens, score) — the accounting identity of one counter."""
    parsed = {}
    for member, score in zset.items():
        total = int(member.rpartition("|")[2])
        parsed[member] = (total, score)
    return parsed


@pytest.fixture
def frozen_now(monkeypatch):
    monkeypatch.setattr(rebuilder, "datetime", FrozenDateTime)
    return FIXED_NOW


async def _run(monkeypatch, rlc, users, keys, rows, resets=(), existing=None):
    # One shared session across the async-with blocks; exec order per cycle
    # is users, keys, then (only when something is missing) resets and rows.
    shared = FakeAsyncSession([users, keys, list(resets), rows])

    def factory():
        return shared

    monkeypatch.setattr(rebuilder, "async_session", factory)
    client = FakeRedis(existing=existing)
    monkeypatch.setattr(
        rebuilder,
        "reader_from_config",
        lambda cfg: SimpleNamespace(client=client),
    )
    rebuild = rebuilder.RateLimitUsageRebuilder(SimpleNamespace(gateway_rate_limit=rlc))
    await rebuild._rebuild(rlc, client)
    return client, shared, rebuild


@pytest.mark.asyncio
async def test_rebuild_restores_lost_counters_from_usage_details(
    frozen_now, monkeypatch
):
    """Day / month / week counters reappear with per-hour members and the
    right attribution: key windows count only their key's rows, user windows
    count every row of the user, and UI traffic (keyless rows) rides the
    user-level buckets only."""
    rlc = _cfg()
    user7, user9 = _user(7, weekly=100_000), _user(9)
    key42 = _key(42, access_key="ak-abc", user_id=7)
    key43 = _key(43, access_key="ak-nine", user_id=9)
    rows = [
        # Today: the key's own day quota and the user's shared windows.
        _row(7, 42, datetime(2026, 11, 19, 9, 0), 100, 50),
        # Keyless UI traffic: user-level windows only.
        _row(7, None, datetime(2026, 11, 19, 10, 30), 10, 5),
        # Three days back: inside the rolling week, outside today.
        _row(7, 42, datetime(2026, 11, 16, 8, 0), 500, 0),
        # Before the month started: outside every rebuilt window.
        _row(7, 42, datetime(2026, 10, 30, 10, 0), 1000, 0),
        # Another user's traffic must not leak into user-7's counters.
        _row(9, 43, datetime(2026, 11, 19, 9, 0), 999, 0),
    ]

    client, _, _ = await _run(monkeypatch, rlc, [user7, user9], [key42, key43], rows)

    # The key's day quota: only key-42's rows, today only.
    day_key = key_windows(rlc, key42)[0].redis_key
    day_members = _tokens(client.zsets[day_key])
    assert day_members == {
        f"rebuild-7-42-{hour_ts(datetime(2026, 11, 19, 9, 0))}|150": (
            150,
            hour_ts(datetime(2026, 11, 19, 9, 0)) + 1,
        )
    }

    # The user's shared week: key + keyless rows, inside the rolling window.
    week_windows = {w.redis_key: w for w in user_limit_windows(rlc, 7, user7)}
    week_key = next(k for k, w in week_windows.items() if w.period == "604800s")
    week_members = _tokens(client.zsets[week_key])
    assert sum(tokens for tokens, _ in week_members.values()) == 665
    assert len(week_members) == 3

    # The user's shared month: the pre-month row stays out.
    month_key = next(k for k, w in week_windows.items() if w.period == "each_month")
    month_members = _tokens(client.zsets[month_key])
    assert sum(tokens for tokens, _ in month_members.values()) == 665

    # The default-plan fallback rides the same user-level aggregates.
    plan_windows = {
        w.redis_key: w for w in default_plan_windows(rlc, 7) if w.period != "60s"
    }
    plan_day = next(k for k, w in plan_windows.items() if w.period == "each_day")
    assert sum(tokens for tokens, _ in _tokens(client.zsets[plan_day]).values()) == 165
    plan_month = next(k for k, w in plan_windows.items() if w.period == "each_month")
    assert (
        sum(tokens for tokens, _ in _tokens(client.zsets[plan_month]).values()) == 665
    )

    # The other user's day quota carries only their own rows.
    day43 = key_windows(rlc, key43)[0].redis_key
    assert sum(tokens for tokens, _ in _tokens(client.zsets[day43]).values()) == 999

    # Minute windows are never rebuilt: no 60s key anywhere.
    assert not any("60s" in key for key in client.zsets)


@pytest.mark.asyncio
async def test_warmup_replaces_a_stale_counter(frozen_now, monkeypatch):
    """The startup pass DELs a surviving counter before writing: a young
    Redis that only saw the last hours of the month gets the database's
    full picture, live members included, not a merge on top of them."""
    rlc = _cfg()
    user7 = _user(7)
    key42 = _key(42, access_key="ak-abc", user_id=7)
    day_key = key_windows(rlc, key42)[0].redis_key
    rows = [_row(7, 42, datetime(2026, 11, 19, 9, 0), 100, 50)]

    client, _, _ = await _run(
        monkeypatch,
        rlc,
        [user7],
        [key42],
        rows,
        existing={day_key},
    )
    # The stale live member the gateway wrote is gone with the counter.
    assert _tokens(client.zsets[day_key]) == {
        f"rebuild-7-42-{hour_ts(datetime(2026, 11, 19, 9, 0))}|150": (
            150,
            hour_ts(datetime(2026, 11, 19, 9, 0)) + 1,
        )
    }


@pytest.mark.asyncio
async def test_steady_state_cycle_never_touches_live_counters(frozen_now, monkeypatch):
    """After the startup pass, a healthy Redis costs one pipelined EXISTS
    per cycle: no database scan, no writes. Only a missing counter draws
    a rebuild, and live counters stay out of it."""
    rlc = _cfg()
    user7 = _user(7)
    key42 = _key(42, access_key="ak-abc", user_id=7)
    rows = [_row(7, 42, datetime(2026, 11, 19, 9, 0), 100, 50)]

    client, shared, rebuild = await _run(monkeypatch, rlc, [user7], [key42], rows)
    snapshot = {key: dict(zset) for key, zset in client.zsets.items()}
    client.existing.update(snapshot)
    calls_after_warmup = shared.calls

    await rebuild._rebuild(rlc, client)

    # Unchanged counters, and the pass stopped at the users / keys reads --
    # the details scan never ran (2 calls per cycle for the specs).
    assert client.zsets == snapshot
    assert shared.calls == calls_after_warmup + 2


@pytest.mark.asyncio
async def test_missing_counter_is_rebuilt_in_steady_state(frozen_now, monkeypatch):
    """A counter lost mid-month (Redis failover) comes back on the next
    cycle from the database, while the counters that survived stay out of
    the batch."""
    rlc = _cfg()
    user7 = _user(7)
    key42 = _key(42, access_key="ak-abc", user_id=7)
    rows = [_row(7, 42, datetime(2026, 11, 19, 9, 0), 100, 50)]

    client, shared, rebuild = await _run(monkeypatch, rlc, [user7], [key42], rows)
    snapshot = {key: dict(zset) for key, zset in client.zsets.items()}
    # Everything survives the failover except the key's day counter.
    client.existing.update(snapshot)
    day_key = key_windows(rlc, key42)[0].redis_key
    client.existing.discard(day_key)
    client.zsets.pop(day_key, None)

    await rebuild._rebuild(rlc, client)

    assert client.zsets[day_key] == snapshot[day_key]
    for key, zset in snapshot.items():
        if key != day_key:
            assert client.zsets[key] == zset


@pytest.mark.asyncio
async def test_reset_cutoffs_keep_forgiven_spend_out(frozen_now, monkeypatch):
    """A reset forgives the spend before it: the weekly counter is rebuilt
    from post-reset rows only, while the daily quota (no reset kind) keeps
    every row inside its window."""
    rlc = _cfg()
    user7 = _user(7, weekly=100_000, monthly=5_000_000)
    key42 = _key(42, access_key="ak-abc", user_id=7)
    rows = [
        # Forgiven by the weekly reset on Nov 18.
        _row(7, 42, datetime(2026, 11, 17, 10, 0), 700, 0),
        # Recorded after both resets.
        _row(7, 42, datetime(2026, 11, 19, 9, 0), 100, 50),
    ]
    resets = [
        _reset(7, "weekly", datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)),
        _reset(7, "monthly", datetime(2026, 11, 15, 12, 0, tzinfo=timezone.utc)),
    ]

    client, _, _ = await _run(monkeypatch, rlc, [user7], [key42], rows, resets=resets)

    week_key = next(
        w.redis_key for w in user_limit_windows(rlc, 7, user7) if w.period == "604800s"
    )
    assert sum(tokens for tokens, _ in _tokens(client.zsets[week_key]).values()) == 150

    # The monthly reset predates both rows, so the month keeps everything.
    month_key = next(
        w.redis_key
        for w in user_limit_windows(rlc, 7, user7)
        if w.period == "each_month"
    )
    assert sum(tokens for tokens, _ in _tokens(client.zsets[month_key]).values()) == 850

    # The daily quota has no reset kind, but the day window only spans the
    # last 24 hours: the Nov 17 row is outside it on its own, reset or not.
    day_key = key_windows(rlc, key42)[0].redis_key
    assert sum(tokens for tokens, _ in _tokens(client.zsets[day_key]).values()) == 150


@pytest.mark.asyncio
async def test_day_boundary_follows_the_deployment_timezone(frozen_now, monkeypatch):
    """Calendar windows anchor to the configured timezone: a row before the
    local midnight is outside the day window even though its UTC stamp sits
    inside a UTC day."""
    rlc = _cfg(timezone="Europe/Berlin")
    user7 = _user(7)
    key42 = _key(42, access_key="ak-abc", user_id=7)
    rows = [
        # 20:00 UTC = 21:00 Berlin on Nov 18 -- before the Berlin day that
        # starts at 23:00 UTC.
        _row(7, 42, datetime(2026, 11, 18, 20, 0), 100, 0),
        # 23:30 UTC = 00:30 Berlin on Nov 19 -- inside.
        _row(7, 42, datetime(2026, 11, 18, 23, 30), 40, 10),
    ]

    client, _, _ = await _run(monkeypatch, rlc, [user7], [key42], rows)

    day_key = key_windows(rlc, key42)[0].redis_key
    day_members = _tokens(client.zsets[day_key])
    assert sum(tokens for tokens, _ in day_members.values()) == 50
    (member,) = day_members
    # The kept bucket is the 23:00 UTC hour of Nov 18.
    assert member == f"rebuild-7-42-{hour_ts(datetime(2026, 11, 18, 23, 0))}|50"
