"""Tests for the startup rebuild of the gateway's Redis rate-limit counters.

The rebuilder must reproduce the plugin's own accounting closely enough to
restore lost quotas: the right keys (user-level buckets on
``gpustack-<id>``), the right token totals per hour bucket, a startup
warm-up that replaces whatever survived, and a hard rule that steady-state
cycles never touch live counters. Resets (``gateway_rate_limit_resets``)
forgive earlier spend, so a rebuilt weekly / monthly counter resumes from
the reset.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from gpustack.config.config import GatewayRateLimitConfig, GatewayRateLimitRedis
from gpustack.schemas.api_keys import ApiKey
from gpustack.schemas.principals import Principal
from gpustack.server import rate_limit_usage_rebuilder as rebuilder
from gpustack.gateway.rate_limit_usage import (
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


def _key(id: int, *, access_key="ak", user_id=7) -> ApiKey:
    return ApiKey(
        id=id,
        name=f"key-{id}",
        user_id=user_id,
        access_key=access_key,
        hashed_secret_key=f"hash-{id}",
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
        self.statements = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def exec(self, statement):
        self.statements.append(statement)
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


def _long_windows(rlc, user) -> dict:
    """The user-level long windows, keyed by period. The default plan's
    month window is included: the fallback is where a user without their
    own monthly budget gets its counter."""
    windows = {}
    for spec in user_limit_windows(rlc, user.id, user) + default_plan_windows(
        rlc, user.id
    ):
        if spec.period != "60s":
            windows.setdefault(spec.period, spec.redis_key)
    return windows


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
    """Week / month counters reappear with per-hour members and the right
    attribution: every window is user-level, so keyed and keyless (UI) rows
    of the user aggregate together, and another user's traffic never leaks
    in."""
    rlc = _cfg()
    user7, user9 = _user(7, weekly=100_000), _user(9)
    key42 = _key(42, access_key="ak-abc", user_id=7)
    key43 = _key(43, access_key="ak-nine", user_id=9)
    rows = [
        # Today: the user's shared windows.
        _row(7, 42, datetime(2026, 11, 19, 9, 0), 100, 50),
        # Keyless UI traffic: rides the same user-level buckets.
        _row(7, None, datetime(2026, 11, 19, 10, 30), 10, 5),
        # Three days back: inside the rolling week, outside today.
        _row(7, 42, datetime(2026, 11, 16, 8, 0), 500, 0),
        # Before the month started: outside every rebuilt window.
        _row(7, 42, datetime(2026, 10, 30, 10, 0), 1000, 0),
        # Another user's traffic must not leak into user-7's counters.
        _row(9, 43, datetime(2026, 11, 19, 9, 0), 999, 0),
    ]

    client, _, _ = await _run(monkeypatch, rlc, [user7, user9], [key42, key43], rows)

    # The user's shared week: keyed + keyless rows, inside the rolling
    # window.
    week_key = _long_windows(rlc, user7)["604800s"]
    week_members = _tokens(client.zsets[week_key])
    assert sum(tokens for tokens, _ in week_members.values()) == 665
    assert len(week_members) == 3

    # The user's shared month: the pre-month row stays out.
    month_key = _long_windows(rlc, user7)["each_month"]
    month_members = _tokens(client.zsets[month_key])
    assert sum(tokens for tokens, _ in month_members.values()) == 665

    # The default-plan fallback rides the same user-level aggregates.
    plan_month = next(
        w.redis_key for w in default_plan_windows(rlc, 7) if w.period == "each_month"
    )
    assert (
        sum(tokens for tokens, _ in _tokens(client.zsets[plan_month]).values()) == 665
    )

    # The other user's month carries only their own rows.
    user9_month = _tokens(client.zsets[_long_windows(rlc, user9)["each_month"]])
    assert sum(tokens for tokens, _ in user9_month.values()) == 999

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
    month_key = _long_windows(rlc, user7)["each_month"]
    rows = [_row(7, 42, datetime(2026, 11, 19, 9, 0), 100, 50)]

    shared = FakeAsyncSession([[user7], [key42], [], rows])
    monkeypatch.setattr(rebuilder, "async_session", lambda: shared)
    monkeypatch.setattr(
        rebuilder,
        "reader_from_config",
        lambda cfg: SimpleNamespace(client=FakeRedis()),
    )
    rebuild = rebuilder.RateLimitUsageRebuilder(SimpleNamespace(gateway_rate_limit=rlc))
    client = FakeRedis(existing={month_key})
    # The gateway's own live member: whatever the young counter counted.
    client.zsets[month_key] = {"<uuid-of-live-request>|77": 5}

    await rebuild._rebuild(rlc, client)

    assert _tokens(client.zsets[month_key]) == {
        f"rebuild-7-{hour_ts(datetime(2026, 11, 19, 9, 0))}|150": (
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
    # Everything survives the failover except the user's month counter.
    client.existing.update(snapshot)
    month_key = _long_windows(rlc, user7)["each_month"]
    client.existing.discard(month_key)
    client.zsets.pop(month_key, None)

    await rebuild._rebuild(rlc, client)

    assert client.zsets[month_key] == snapshot[month_key]
    for key, zset in snapshot.items():
        if key != month_key:
            assert client.zsets[key] == zset


@pytest.mark.asyncio
async def test_reset_cutoffs_keep_forgiven_spend_out(frozen_now, monkeypatch):
    """A reset forgives the spend before it: the weekly counter is rebuilt
    from post-reset rows only, while the monthly counter (whose reset
    predates every row) keeps everything inside its window."""
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


@pytest.mark.asyncio
async def test_failed_warmup_retries_as_warmup(frozen_now, monkeypatch):
    """A warm-up that dies mid-pass (database hiccup) must not be
    remembered as done: the next cycle replaces the counters again. A
    consumed-but-failed startup pass would otherwise leave a surviving
    counter with lost history uncorrected forever."""
    rlc = _cfg()
    user7 = _user(7)
    key42 = _key(42, access_key="ak-abc", user_id=7)
    rows = [_row(7, 42, datetime(2026, 11, 19, 9, 0), 100, 50)]

    rebuild = rebuilder.RateLimitUsageRebuilder(SimpleNamespace(gateway_rate_limit=rlc))
    shared = FakeAsyncSession([[user7], [key42], [], rows])
    monkeypatch.setattr(rebuilder, "async_session", lambda: shared)
    client = FakeRedis()
    monkeypatch.setattr(
        rebuilder,
        "reader_from_config",
        lambda cfg: SimpleNamespace(client=client),
    )

    failing = {"calls": 0}
    original_load = rebuilder.RateLimitUsageRebuilder._load_usage_buckets

    async def flaky_load(self, *args):
        failing["calls"] += 1
        if failing["calls"] == 1:
            raise RuntimeError("database hiccup")
        return await original_load(self, *args)

    monkeypatch.setattr(
        rebuilder.RateLimitUsageRebuilder, "_load_usage_buckets", flaky_load
    )
    with pytest.raises(RuntimeError):
        await rebuild._rebuild(rlc, client)
    assert rebuild._warmed is False

    # The retry is still a warm-up: it DELs the stale counter instead of
    # merging into it.
    shared = FakeAsyncSession([[user7], [key42], [], rows])
    monkeypatch.setattr(rebuilder, "async_session", lambda: shared)
    month_key = _long_windows(rlc, user7)["each_month"]
    client.existing.add(month_key)
    await rebuild._rebuild(rlc, client)
    assert rebuild._warmed is True
    assert _tokens(client.zsets[month_key])


@pytest.mark.asyncio
async def test_scan_covers_rows_dated_outside_the_utc_day(frozen_now, monkeypatch):
    """``model_usage_details.date`` holds the server's local date, which
    runs a day ahead of UTC on a +08:00 server with a UTC gateway: rows of
    the locally-current day must stay inside the scan range."""
    rlc = _cfg()
    user7 = _user(7)
    key42 = _key(42, access_key="ak-abc", user_id=7)
    # 11:30 UTC = 00:30 local next day: the row is dated tomorrow-UTC.
    rows = [_row(7, 42, datetime(2026, 11, 19, 11, 30), 300, 0)]

    client, shared, rebuild = await _run(monkeypatch, rlc, [user7], [key42], rows)

    # The scan's upper date bound is padded past the UTC date.
    rows_statement = next(
        str(s.compile(compile_kwargs={"literal_binds": True}))
        for s in shared.statements
        if "model_usage_details" in str(s)
    )
    assert "2026-11-20" in rows_statement

    # And the row itself landed in the user's month counter.
    month_key = _long_windows(rlc, user7)["each_month"]
    assert sum(tokens for tokens, _ in _tokens(client.zsets[month_key]).values()) == 300


@pytest.mark.asyncio
async def test_month_boundary_follows_the_deployment_timezone(frozen_now, monkeypatch):
    """Calendar windows anchor to the configured timezone: a row before the
    local month start is outside the window even though a UTC-month filter
    would treat it the same, and a row past the local start stays inside
    even though its UTC date sits in the previous UTC month."""
    rlc = _cfg(timezone="Europe/Berlin")
    user7 = _user(7)
    key42 = _key(42, access_key="ak-abc", user_id=7)
    rows = [
        # Oct 31 22:30 UTC = 23:30 Berlin on Oct 31 -- before the Berlin
        # month that starts Nov 1, 00:00 Berlin (Oct 31 23:00 UTC).
        _row(7, 42, datetime(2026, 10, 31, 22, 30), 100, 0),
        # Oct 31 23:30 UTC = Nov 1, 00:30 Berlin -- inside, though its UTC
        # date is Oct 31.
        _row(7, 42, datetime(2026, 10, 31, 23, 30), 40, 10),
    ]

    client, _, _ = await _run(monkeypatch, rlc, [user7], [key42], rows)

    month_key = _long_windows(rlc, user7)["each_month"]
    month_members = _tokens(client.zsets[month_key])
    assert sum(tokens for tokens, _ in month_members.values()) == 50
    (member,) = month_members
    # The kept bucket is the 23:00 UTC hour of Oct 31.
    assert member == f"rebuild-7-{hour_ts(datetime(2026, 10, 31, 23, 0))}|50"
