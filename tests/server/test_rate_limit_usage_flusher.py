"""The rate-limit usage flusher: discovery, attribution, upserts and no-ops.

Offline by construction: Redis is a fake exposing only SCAN and ZRANGEBYSCORE
(the exact call signatures the flush cycle uses), the database is a fake
session dispatching the flusher's SELECT shapes, and the clock is frozen so
the window-start math is deterministic.
"""

import asyncio
import fnmatch
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from redis.exceptions import RedisError
from sqlalchemy import Delete, Select
from sqlalchemy.sql.elements import BooleanClauseList

from gpustack.config.config import GatewayRateLimitConfig, GatewayRateLimitRedis
from gpustack.gateway.rate_limit import rate_limit_plugin_name
from gpustack.gateway.rate_limit_usage import RateLimitUsageReader
from gpustack.schemas.api_keys import ApiKey
from gpustack.schemas.gateway_rate_limit_usages import GatewayRateLimitUsage
from gpustack.schemas.principals import Principal
from gpustack.server import rate_limit_usage_flusher as flusher_module
from gpustack.server.rate_limit_usage_flusher import (
    ParsedKey,
    RateLimitUsageFlusher,
    parse_rate_limit_key,
)

# A fixed instant with a 12h elapsed UTC day and a fresh month, so the
# calendar window starts are exact regardless of when the suite runs.
FIXED_NOW = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
NOW_TS = int(FIXED_NOW.timestamp())

DAY_KEY = f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|t:each_day"
MONTH_KEY = f"{rate_limit_plugin_name}|user-7|consumer=gpustack-7|t:each_month"
WEEK_KEY = f"{rate_limit_plugin_name}|key-42|consumer=ak-abc.gpustack-7|t:604800s"


def _cfg(**overrides) -> GatewayRateLimitConfig:
    fields = dict(
        enabled=True,
        redis=GatewayRateLimitRedis(service_name="rate-limit-redis", password="pw"),
        timezone="UTC",
        default_rpm=10,
        default_tpm=60_000,
        default_daily_tokens=2_000_000,
        default_monthly_tokens=20_000_000,
        usage_flush_interval_seconds=60,
    )
    fields.update(overrides)
    return GatewayRateLimitConfig(**fields)


def _user(id, *, weekly=None, monthly=None) -> Principal:
    return Principal(
        id=id,
        name=f"user-{id}",
        weekly_token_limit=weekly,
        monthly_token_limit=monthly,
    )


def _key(id, *, access_key="ak", user_id=7, rpm_limit=None) -> ApiKey:
    stamp = FIXED_NOW.replace(tzinfo=None)
    return ApiKey(
        id=id,
        name=f"key-{id}",
        user_id=user_id,
        access_key=access_key,
        hashed_secret_key=f"hash-{id}",
        rpm_limit=rpm_limit,
        created_at=stamp,
        updated_at=stamp,
    )


class FakeRedis:
    """Stand-in for redis.asyncio.Redis exposing SCAN and ZRANGEBYSCORE.

    Seeded as ``{key: [(score, member), ...]}``; SCAN filters by fnmatch.
    The zrangebyscore bound parsing mirrors redis-py: a leading ``(`` marks
    the bound exclusive, bare numbers inclusive. With ``fail_reads`` set,
    every ZRANGEBYSCORE raises.
    """

    def __init__(self, members, *, fail_reads=False):
        self._members = members
        self._fail_reads = fail_reads
        self.scan_calls = []

    async def scan(self, cursor=0, match=None, count=None):
        self.scan_calls.append((cursor, match, count))
        hits = [
            key for key in self._members if match is None or fnmatch.fnmatch(key, match)
        ]
        return 0, hits

    @staticmethod
    def _bound(value):
        if isinstance(value, str) and value.startswith("("):
            return float(value[1:]), True
        return float(value), False

    async def zrangebyscore(self, key, lo, hi, withscores=False):
        if self._fail_reads:
            raise RedisError("dial tcp: redis-host:6379")
        lo_value, lo_exclusive = self._bound(lo)
        hi_value, hi_exclusive = self._bound(hi)
        hits = [
            (member, score)
            for score, member in self._members.get(key, [])
            if (score > lo_value if lo_exclusive else score >= lo_value)
            and (score < hi_value if hi_exclusive else score <= hi_value)
        ]
        hits.sort(key=lambda pair: pair[1])
        if not withscores:
            return [member for member, _ in hits]
        return hits


def _eq_filters(statement):
    """{column: value} pairs of a statement's top-level == comparisons."""
    clause = statement.whereclause
    if clause is None:
        return {}
    parts = list(clause.clauses) if isinstance(clause, BooleanClauseList) else [clause]
    return {part.left.name: part.right.value for part in parts}


class FakeSession:
    """Stand-in for the async session: dispatches the flusher's SELECT
    shapes (api keys, principals, usage lookups) and records every write.

    The usage lookup emulates its WHERE clause against the in-memory rows,
    which is what lets the second flush's SELECT find the first flush's
    INSERTs and take the UPDATE branch.
    """

    def __init__(self, users=(), keys=(), usages=()):
        self._users = list(users)
        self._keys = list(keys)
        self.usages = list(usages)
        self.added = []
        self.statements = []
        self.commit_calls = 0

    @staticmethod
    def _result(rows):
        return SimpleNamespace(
            all=lambda: list(rows), first=lambda: rows[0] if rows else None
        )

    async def exec(self, statement):
        self.statements.append(statement)
        if isinstance(statement, Select):
            return self._select(statement)
        if isinstance(statement, Delete):
            return self._delete(statement)
        raise AssertionError(f"unexpected statement: {statement}")

    def _select(self, statement):
        entity = statement.column_descriptions[0]["entity"]
        if entity is ApiKey:
            return self._result(self._keys)
        if entity is Principal:
            return self._result(self._users)
        if entity is GatewayRateLimitUsage:
            filters = _eq_filters(statement)
            hits = [
                row
                for row in self.usages
                if all(
                    getattr(row, column) == value for column, value in filters.items()
                )
            ]
            return self._result(hits)
        raise AssertionError(f"unexpected select entity: {entity}")

    def _delete(self, statement):
        clause = statement.whereclause
        column = clause.left.name
        cutoff = clause.right.value
        self.usages = [
            row
            for row in self.usages
            if not (getattr(row, column) is not None and getattr(row, column) < cutoff)
        ]
        return SimpleNamespace()

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.commit_calls += 1
        for obj in self.added:
            if obj not in self.usages:
                self.usages.append(obj)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


class FrozenDateTime(datetime):
    """``datetime.now()`` pinned to FIXED_NOW; injected over the flusher
    module's ``datetime``."""

    @classmethod
    def now(cls, tz=None):
        if tz is not None:
            return FIXED_NOW.astimezone(tz)
        return FIXED_NOW.replace(tzinfo=None)


@pytest.fixture
def frozen_now(monkeypatch):
    monkeypatch.setattr(flusher_module, "datetime", FrozenDateTime)
    return FIXED_NOW


def _flusher(cfg) -> RateLimitUsageFlusher:
    return RateLimitUsageFlusher(SimpleNamespace(gateway_rate_limit=cfg))


async def _run_cycle(monkeypatch, flusher, redis, session):
    monkeypatch.setattr(
        flusher_module,
        "reader_from_config",
        lambda cfg: RateLimitUsageReader(redis),
    )
    monkeypatch.setattr(flusher_module, "async_session", lambda: session)
    await flusher._flush_once()


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (
            f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|t:each_day",
            ParsedKey(
                combo="default-plan",
                kind="token_calendar",
                period="each_day",
                window_seconds=None,
                identity="gpustack-7",
            ),
        ),
        (
            f"{rate_limit_plugin_name}|user-6|consumer=gpustack-6|t:604800s",
            ParsedKey(
                combo="user-6",
                kind="token_rolling",
                period="604800s",
                window_seconds=604800,
                identity="gpustack-6",
            ),
        ),
        (
            f"{rate_limit_plugin_name}|key-42|consumer=ak-abc.gpustack-6|t:each_month",
            ParsedKey(
                combo="key-42",
                kind="token_calendar",
                period="each_month",
                window_seconds=None,
                identity="ak-abc.gpustack-6",
            ),
        ),
        (
            f"{rate_limit_plugin_name}|key-42|consumer=ak-abc.gpustack-6|q:60s",
            ParsedKey(
                combo="key-42",
                kind="query",
                period="60s",
                window_seconds=60,
                identity="ak-abc.gpustack-6",
            ),
        ),
        # Extra dim fragments around the consumer one are tolerated.
        (
            f"{rate_limit_plugin_name}|default-plan|cluster=c1|consumer=gpustack-7"
            "|t:each_day",
            ParsedKey(
                combo="default-plan",
                kind="token_calendar",
                period="each_day",
                window_seconds=None,
                identity="gpustack-7",
            ),
        ),
        ("other-rule|default-plan|consumer=gpustack-7|t:each_day", None),
        (f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|t:60x", None),
        (f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|x:60s", None),
        (f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|q:each_day", None),
        (
            f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|t:each_week",
            None,
        ),
        (f"{rate_limit_plugin_name}|default-plan|t:each_day", None),
        (
            f"{rate_limit_plugin_name}|default-plan|consumer=a|consumer=b|t:each_day",
            None,
        ),
        (f"{rate_limit_plugin_name}|default-plan|consumer=|t:each_day", None),
    ],
)
def test_parse_rate_limit_key(key, expected):
    assert parse_rate_limit_key(key) == expected


def _seed(members_by_key):
    return {
        key: [(NOW_TS - offset, f"{member}|{count}") for offset, member, count in rows]
        for key, rows in members_by_key.items()
    }


@pytest.mark.asyncio
async def test_flush_upserts_token_windows(frozen_now, monkeypatch):
    """Day / month / week windows land as three attributed rows: calendar
    starts anchor to the timezone boundary, the rolling week to the shared
    now, and quotas resolve from the owning key's and user's budgets."""
    redis = FakeRedis(
        _seed(
            {
                DAY_KEY: [(100, "a", 700), (5, "b", 534)],
                MONTH_KEY: [(60, "m", 5678)],
                WEEK_KEY: [(30, "w", 910)],
            }
        )
    )
    session = FakeSession(
        users=[_user(7, weekly=100_000, monthly=200_000)],
        keys=[_key(42, access_key="ak-abc", user_id=7)],
    )
    await _run_cycle(monkeypatch, _flusher(_cfg()), redis, session)

    assert session.commit_calls == 1
    by_window = {(row.combo_name, row.kind, row.period): row for row in session.added}
    assert len(by_window) == 3

    day = by_window[("default-plan", "token_calendar", "each_day")]
    assert day.window_start == datetime(2026, 11, 19, 0, 0, tzinfo=timezone.utc)
    assert day.user_id == 7 and day.api_key_id == 0
    assert day.usage == 1234 and day.quota == 2_000_000
    assert day.window_seconds is None

    month = by_window[("user-7", "token_calendar", "each_month")]
    assert month.window_start == datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc)
    assert month.user_id == 7 and month.api_key_id == 0
    assert month.usage == 5678 and month.quota == 200_000

    week = by_window[("key-42", "token_rolling", "604800s")]
    assert week.window_start == FIXED_NOW - timedelta(seconds=604800)
    assert week.user_id == 7 and week.api_key_id == 42
    assert week.usage == 910 and week.quota == 100_000
    assert week.window_seconds == 604800

    # The key batch reads only live rows.
    assert "api_keys.deleted_at IS NULL" in str(session.statements[0])


@pytest.mark.asyncio
async def test_second_flush_updates_the_same_rows(frozen_now, monkeypatch):
    """A repeated cycle upserts: the second flush finds the first flush's
    rows through the unique tuple and UPDATEs them — no duplicates."""
    redis = FakeRedis(_seed({DAY_KEY: [(100, "a", 700)], MONTH_KEY: [(60, "m", 5678)]}))
    session = FakeSession(users=[_user(7, monthly=200_000)])
    flusher = _flusher(_cfg())
    await _run_cycle(monkeypatch, flusher, redis, session)

    # Usage moved on in Redis between cycles.
    redis._members[DAY_KEY] = [(NOW_TS - 100, "a|999")]
    await _run_cycle(monkeypatch, flusher, redis, session)

    assert len(session.added) == 2
    assert session.commit_calls == 2
    assert len(session.usages) == 2
    day = next(row for row in session.usages if row.combo_name == "default-plan")
    assert day.usage == 999
    assert day.updated_at == FIXED_NOW


@pytest.mark.asyncio
async def test_query_windows_are_not_persisted(frozen_now, monkeypatch):
    """Only token windows land: the per-minute request counter is skipped
    while the same combo's per-minute token window is kept."""
    redis = FakeRedis(
        _seed(
            {
                f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|q:60s": [
                    (10, "q", 4)
                ],
                f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|t:60s": [
                    (10, "t", 500)
                ],
            }
        )
    )
    session = FakeSession(users=[_user(7)])
    await _run_cycle(monkeypatch, _flusher(_cfg()), redis, session)

    assert [(row.kind, row.period, row.usage) for row in session.added] == [
        ("token_rolling", "60s", 500)
    ]


@pytest.mark.asyncio
async def test_unparseable_keys_touch_nothing(frozen_now, monkeypatch):
    """A sweep finding only off-layout keys opens no session at all."""
    redis = FakeRedis(
        _seed(
            {
                "other-rule|default-plan|consumer=gpustack-7|t:each_day": [(5, "x", 1)],
                f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|t:60x": [
                    (5, "x", 1)
                ],
                f"{rate_limit_plugin_name}|default-plan|t:each_day": [(5, "x", 1)],
            }
        )
    )
    session = FakeSession(users=[_user(7)])
    await _run_cycle(monkeypatch, _flusher(_cfg()), redis, session)

    assert session.added == []
    assert session.statements == []
    assert session.commit_calls == 0


@pytest.mark.asyncio
async def test_unknown_combo_is_skipped_without_raising(frozen_now, monkeypatch):
    """A combo segment nothing attributes is dropped beside a valid key: the
    cycle continues and the recognizable windows still land."""
    redis = FakeRedis(
        _seed(
            {
                f"{rate_limit_plugin_name}|weird|consumer=gpustack-7|t:each_day": [
                    (10, "x", 1)
                ],
                DAY_KEY: [(100, "a", 700)],
            }
        )
    )
    session = FakeSession(users=[_user(7)])
    await _run_cycle(monkeypatch, _flusher(_cfg()), redis, session)

    assert [row.combo_name for row in session.added] == ["default-plan"]


@pytest.mark.asyncio
async def test_read_failure_leaves_db_untouched(frozen_now, monkeypatch, caplog):
    """A failed batch read drops the whole cycle — no upserts, no commit —
    and only the exception's class name surfaces in the warning."""
    redis = FakeRedis(_seed({DAY_KEY: [(100, "a", 700)]}), fail_reads=True)
    session = FakeSession(users=[_user(7)])
    with caplog.at_level(logging.WARNING):
        await _run_cycle(monkeypatch, _flusher(_cfg()), redis, session)

    assert session.added == []
    assert session.commit_calls == 0
    assert "RedisError" in caplog.text
    assert "redis-host" not in caplog.text


@pytest.mark.asyncio
async def test_prune_drops_stale_snapshots(frozen_now, monkeypatch):
    """Snapshots untouched for past the retention window are deleted in the
    same pass; a fresh row is upserted in place."""
    old = GatewayRateLimitUsage(
        combo_name="default-plan",
        kind="token_calendar",
        period="each_day",
        window_start=FIXED_NOW - timedelta(days=95),
        user_id=7,
        api_key_id=0,
        usage=1,
        updated_at=FIXED_NOW - timedelta(days=91),
    )
    fresh = GatewayRateLimitUsage(
        combo_name="default-plan",
        kind="token_calendar",
        period="each_day",
        window_start=datetime(2026, 11, 19, 0, 0, tzinfo=timezone.utc),
        user_id=7,
        api_key_id=0,
        usage=2,
        updated_at=FIXED_NOW - timedelta(days=1),
    )
    redis = FakeRedis(_seed({DAY_KEY: [(100, "a", 700)]}))
    session = FakeSession(users=[_user(7)], usages=[old, fresh])
    await _run_cycle(monkeypatch, _flusher(_cfg()), redis, session)

    assert all(row is not old for row in session.usages)
    # The fresh row matched the upsert tuple and was updated, not duplicated.
    assert any(row is fresh for row in session.usages)
    assert fresh.usage == 700
    assert fresh.updated_at == FIXED_NOW
    assert fresh.quota == 2_000_000


@pytest.mark.parametrize(
    "rlc",
    [
        _cfg(enabled=False),
        _cfg(redis=None),
        _cfg(usage_flush_interval_seconds=0),
    ],
)
@pytest.mark.asyncio
async def test_start_noops_without_touching_redis(rlc, monkeypatch):
    """Disabled, Redis-less and zero-interval configs all idle out before
    the loop — no reader is ever built."""

    def _forbid(cfg):
        raise AssertionError("a no-op flusher must not dial Redis")

    monkeypatch.setattr(flusher_module, "reader_from_config", _forbid)
    await asyncio.wait_for(
        RateLimitUsageFlusher(SimpleNamespace(gateway_rate_limit=rlc)).start(),
        timeout=1,
    )
