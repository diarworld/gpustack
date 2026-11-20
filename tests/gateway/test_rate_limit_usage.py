"""Redis-key reconstruction for the rate-limit usage reader.

The strings here are the plugin's documented key layout; a mismatch means the
endpoint would read keys the plugin never writes.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Dict, Optional

import pytest

from gpustack.config.config import GatewayRateLimitConfig, GatewayRateLimitRedis
from gpustack.gateway import rate_limit_usage
from gpustack.gateway.rate_limit import WEEK_IN_SECONDS, rate_limit_plugin_name
from gpustack.gateway.rate_limit_usage import (
    RateLimitUsageReader,
    RateLimitWindowSpec,
    calendar_elapsed_seconds,
    calendar_window_seconds,
    default_plan_windows,
    expected_windows,
    model_concurrency_specs,
    parse_member_count,
    read_concurrency,
    read_windows,
    reader_from_config,
    source_for,
    user_limit_combo,
    user_limit_windows,
)
from gpustack.security import gateway_consumer_identity


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


@dataclass
class _KeyStub:
    """The ApiKey attributes the window reconstruction reads."""

    id: int
    access_key: Optional[str]
    user_id: int
    daily_token_limit: Optional[int] = None


@dataclass
class _UserStub:
    """The Principal attributes the user-level window reconstruction reads."""

    rpm_limit: Optional[int] = None
    tpm_limit: Optional[int] = None
    weekly_token_limit: Optional[int] = None
    monthly_token_limit: Optional[int] = None
    is_admin: bool = False
    model_concurrency_limits: Optional[Dict[str, int]] = None


def _shapes(windows):
    return [(w.kind, w.period, w.window_seconds, w.limit, w.redis_key) for w in windows]


def _spec(**overrides) -> RateLimitWindowSpec:
    fields = dict(
        kind="query", period="60s", window_seconds=60, limit=10, redis_key="k"
    )
    fields.update(overrides)
    return RateLimitWindowSpec(**fields)


class _FakeRedis:
    """Stand-in for redis.asyncio.Redis exposing only ZRANGEBYSCORE.

    Seeded as ``{key: [(score, member), ...]}``. Bound parsing mirrors
    redis-py: a leading ``(`` marks the bound exclusive, bare numbers are
    inclusive.
    """

    def __init__(self, members):
        self._members = members

    @staticmethod
    def _bound(value):
        if isinstance(value, str) and value.startswith("("):
            return float(value[1:]), True
        return float(value), False

    def _zrangebyscore_sync(self, key, lo, hi, withscores=False):
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

    async def zrangebyscore(self, key, lo, hi, withscores=False):
        return self._zrangebyscore_sync(key, lo, hi, withscores)

    def pipeline(self, transaction=False):
        return _FakePipeline(self)


class _FakePipeline:
    """Queues ZRANGEBYSCORE / ZCARD commands and replays them on ``execute``
    — the slice of redis-py's pipeline the batched readers use."""

    def __init__(self, client):
        self._client = client
        self._commands = []

    def zrangebyscore(self, key, lo, hi, withscores=False):
        self._commands.append(("zrangebyscore", key, lo, hi, withscores))
        return self

    def zcard(self, key):
        self._commands.append(("zcard", key))
        return self

    async def execute(self, raise_on_error=True):
        results = []
        for command in self._commands:
            try:
                results.append(self._execute_one(command))
            except Exception as exc:
                if raise_on_error:
                    raise
                results.append(exc)
        return results

    def _execute_one(self, command):
        kind = command[0]
        if kind == "zrangebyscore":
            _, key, lo, hi, withscores = command
            return self._client._zrangebyscore_sync(key, lo, hi, withscores)
        _, key = command
        return len(self._client._members.get(key, []))


def test_default_plan_windows_are_the_fallback_set():
    windows = default_plan_windows(_cfg(), 6)
    prefix = f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-6"
    assert _shapes(windows) == [
        ("query", "60s", 60, 10, f"{prefix}|q:60s"),
        ("token_rolling", "60s", 60, 60_000, f"{prefix}|t:60s"),
        ("token_calendar", "each_day", None, 2_000_000, f"{prefix}|t:each_day"),
        ("token_calendar", "each_month", None, 20_000_000, f"{prefix}|t:each_month"),
    ]


def test_user_windows_carry_deployment_defaults():
    """A user without settings still gets the deployment plan: the key
    combos their requests also match suppress the fallback default plan, so
    inheriting is what keeps rpm/tpm/monthly in force."""
    windows = expected_windows(_cfg(), 6, None, None)
    prefix = f"{rate_limit_plugin_name}|user-6|consumer=gpustack-6"
    assert _shapes(windows) == [
        ("query", "60s", 60, 10, f"{prefix}|q:60s"),
        ("token_rolling", "60s", 60, 60_000, f"{prefix}|t:60s"),
        ("token_calendar", "each_month", None, 20_000_000, f"{prefix}|t:each_month"),
    ]


def test_user_windows_override_defaults_and_add_weekly():
    user = _UserStub(
        rpm_limit=30,
        tpm_limit=200_000,
        weekly_token_limit=100_000,
        monthly_token_limit=200_000_000,
    )
    windows = expected_windows(_cfg(), 6, None, user)
    prefix = f"{rate_limit_plugin_name}|user-6|consumer=gpustack-6"
    assert _shapes(windows) == [
        ("query", "60s", 60, 30, f"{prefix}|q:60s"),
        ("token_rolling", "60s", 60, 200_000, f"{prefix}|t:60s"),
        ("token_rolling", "604800s", WEEK_IN_SECONDS, 100_000, f"{prefix}|t:604800s"),
        (
            "token_calendar",
            "each_month",
            None,
            200_000_000,
            f"{prefix}|t:each_month",
        ),
    ]


def test_key_adds_its_own_day_quota_on_top():
    """The user combo ANDs with the key combo: the shared caps bind every
    key, and the key's day quota buckets on its full consumer string."""
    key = _KeyStub(id=42, access_key="ak-abc123", user_id=6)
    user = _UserStub(weekly_token_limit=100_000)
    windows = expected_windows(_cfg(), 6, key, user)
    identity = gateway_consumer_identity("ak-abc123", 6)
    # Pin the documented identity format: the consumer string ext-auth
    # injects for an API-key request is <access_key>.gpustack-<user_id>.
    assert identity == "ak-abc123.gpustack-6"
    user_prefix = f"{rate_limit_plugin_name}|user-6|consumer=gpustack-6"
    key_prefix = f"{rate_limit_plugin_name}|key-42|consumer={identity}"
    assert _shapes(windows) == [
        ("query", "60s", 60, 10, f"{user_prefix}|q:60s"),
        ("token_rolling", "60s", 60, 60_000, f"{user_prefix}|t:60s"),
        (
            "token_rolling",
            "604800s",
            WEEK_IN_SECONDS,
            100_000,
            f"{user_prefix}|t:604800s",
        ),
        (
            "token_calendar",
            "each_month",
            None,
            20_000_000,
            f"{user_prefix}|t:each_month",
        ),
        # The key's own day quota inherits the deployment default.
        ("token_calendar", "each_day", None, 2_000_000, f"{key_prefix}|t:each_day"),
    ]


def test_key_daily_limit_overrides_default():
    key = _KeyStub(id=7, access_key="ak-x", user_id=6, daily_token_limit=1_000)
    windows = expected_windows(_cfg(), 6, key, None)
    key_windows = [w for w in windows if w.redis_key.split("|")[1] == "key-7"]
    assert [(w.kind, w.period, w.limit) for w in key_windows] == [
        ("token_calendar", "each_day", 1_000)
    ]


def test_legacy_token_key_governs_user_windows_only():
    """Deployment-token rows never bind a key combo: the reconciler skips
    them when publishing combos, so only the user combo governs their
    requests."""
    key = _KeyStub(id=42, access_key="", user_id=6)
    windows = expected_windows(_cfg(), 6, key, None)
    assert all("key-42|" not in w.redis_key for w in windows)
    # Same window set an unkeyed request path gets.
    assert [w.redis_key for w in windows] == [
        w.redis_key for w in expected_windows(_cfg(), 6, None, None)
    ]


def test_source_for():
    key = _KeyStub(id=1, access_key="ak-1", user_id=6)
    legacy = _KeyStub(id=2, access_key="", user_id=6)
    assert source_for(key) == "user+key"
    assert source_for(legacy) == "user+key"
    assert source_for(None) == "user"


def test_model_concurrency_specs_key_layout():
    """The concurrency keys must be byte-identical to what the plugin
    writes: the user limit combo, the captured user segment, and the
    ``c:<model>`` suffix the slot ZSET lives under."""
    user = _UserStub(model_concurrency_limits={"glm-5.3-flash": 4})
    specs = model_concurrency_specs(_cfg(), 42, user)
    assert len(specs) == 1
    assert specs[0].model == "glm-5.3-flash"
    assert specs[0].limit == 4
    assert specs[0].redis_key == (
        f"{rate_limit_plugin_name}|user-42|consumer=gpustack-42|c:glm-5.3-flash"
    )


def test_model_concurrency_specs_are_sorted_and_override_only():
    """Deterministic order for the wire payload, and only the models the
    row names: every other model inherits the deployment default and has
    no itemized counter to read."""
    user = _UserStub(
        model_concurrency_limits={"qwen3.8-27b": 2, "glm-5.3-flash": 4, "free": 0}
    )
    specs = model_concurrency_specs(_cfg(), 42, user)
    assert [(s.model, s.limit) for s in specs] == [
        ("free", 0),
        ("glm-5.3-flash", 4),
        ("qwen3.8-27b", 2),
    ]


def test_model_concurrency_specs_admin_reports_nothing():
    """Admins are exempt from the cap, so their row reports no enforced
    per-model limits regardless of what its map holds."""
    user = _UserStub(is_admin=True, model_concurrency_limits={"glm": 4})
    assert model_concurrency_specs(_cfg(), 42, user) == []


def test_model_concurrency_specs_without_overrides_is_empty():
    assert model_concurrency_specs(_cfg(), 42, _UserStub()) == []


@pytest.mark.asyncio
async def test_concurrency_counts_pipeline_zcard():
    """One pipelined ZCARD per key returns the live slot count -- the same
    number the gateway's check compares against."""
    key_a = f"{rate_limit_plugin_name}|user-42|consumer=gpustack-42|c:glm"
    key_b = f"{rate_limit_plugin_name}|user-42|consumer=gpustack-42|c:qwen"
    client = _FakeRedis(
        {
            key_a: [(1000, "req-1|1"), (1005, "req-2|1")],
            # key_b exists but holds no members: an empty slot set reads 0,
            # not a missing key.
            key_b: [],
        }
    )
    counts = await RateLimitUsageReader(client).concurrency_counts([key_a, key_b])
    assert counts == [2, 0]


@pytest.mark.asyncio
async def test_read_concurrency_fails_open():
    class _BrokenRedis(_FakeRedis):
        def pipeline(self, transaction=False):
            raise ConnectionError("down")

    counts, error = await read_concurrency(
        RateLimitUsageReader(_BrokenRedis({})), ["k1"]
    )
    assert counts is None
    assert error == "ConnectionError"


@pytest.mark.asyncio
async def test_read_concurrency_empty_keys_is_a_clean_noop():
    counts, error = await read_concurrency(RateLimitUsageReader(_FakeRedis({})), [])
    assert counts == []
    assert error is None


def test_calendar_window_seconds_utc():
    now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
    assert calendar_window_seconds("each_day", now, "UTC") == 43_200
    # Nov 19 12:00Z to Dec 1 00:00Z is 11.5 days.
    assert calendar_window_seconds("each_month", now, "UTC") == 993_600
    # Nov 19 12:00Z to Jan 1 00:00Z is 42.5 days.
    assert calendar_window_seconds("each_year", now, "UTC") == 3_672_000


def test_calendar_window_seconds_honours_timezone():
    now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
    # 12:00Z is 13:00 in Berlin (UTC+1 in November), so midnight there is
    # 11 hours away.
    assert calendar_window_seconds("each_day", now, "Europe/Berlin") == 39_600


def test_calendar_window_seconds_rejects_rolling_periods():
    with pytest.raises(ValueError):
        calendar_window_seconds(
            "60s", datetime(2026, 11, 19, tzinfo=timezone.utc), "UTC"
        )


def test_calendar_window_seconds_rejects_naive_datetime():
    """A naive stamp would be silently read as server-local time, shifting
    every calendar boundary."""
    with pytest.raises(ValueError):
        calendar_window_seconds("each_day", datetime(2026, 11, 19, 12, 0, 0), "UTC")


def test_calendar_elapsed_seconds():
    now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
    assert calendar_elapsed_seconds("each_day", now, "UTC") == 43_200
    # Nov 1 00:00Z to Nov 19 12:00Z is 18.5 days.
    assert calendar_elapsed_seconds("each_month", now, "UTC") == 1_598_400
    # Jan 1 00:00Z to Nov 19 12:00Z is 322.5 days in a non-leap year.
    assert calendar_elapsed_seconds("each_year", now, "UTC") == 27_864_000


def test_calendar_elapsed_seconds_complements_window_seconds():
    """Elapsed plus remaining is the period's full length, so the reader can
    bound a calendar window by the elapsed part alone."""
    now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
    elapsed = calendar_elapsed_seconds("each_day", now, "UTC")
    remaining = calendar_window_seconds("each_day", now, "UTC")
    assert elapsed + remaining == 86_400


def test_calendar_elapsed_seconds_rejects_naive_datetime():
    with pytest.raises(ValueError):
        calendar_elapsed_seconds("each_day", datetime(2026, 11, 19, 12, 0, 0), "UTC")


def test_no_windows_when_nothing_configured():
    rlc = _cfg(
        default_rpm=None,
        default_tpm=None,
        default_daily_tokens=None,
        default_monthly_tokens=None,
    )
    assert expected_windows(rlc, 6, None, None) == []


def test_unset_defaults_skip_their_windows():
    rlc = _cfg(default_tpm=None, default_daily_tokens=None, default_monthly_tokens=None)
    windows = expected_windows(rlc, 6, None, None)
    assert [(w.kind, w.period) for w in windows] == [("query", "60s")]


@pytest.mark.asyncio
async def test_window_state_sums_member_counts():
    key = "k"
    now = 1_000
    client = _FakeRedis(
        {
            key: [
                # The plugin prunes members scored at or before
                # now - window_seconds before summing: both of these sit on
                # or below the read's exclusive lower bound.
                (939, "pruned-old|9"),
                (940, "pruned-boundary|4"),
                # redis-py hands scores back as floats; the arithmetic must
                # not care.
                (970.0, b"legacy"),
                (999, "last|5"),
                (1_000, "edge|2"),
                (1_001, "future|100"),
            ]
        }
    )
    state = await RateLimitUsageReader(client).window_state(
        key, window_seconds=60, now=now
    )
    assert state.used == 8


@pytest.mark.asyncio
async def test_window_state_reset_from_oldest():
    client = _FakeRedis({"k": [(945, "a|1"), (970, "b|2"), (999, "c|3")]})
    state = await RateLimitUsageReader(client).window_state(
        "k", window_seconds=60, now=1_000
    )
    assert state.used == 6
    # The count drops when the oldest in-window member leaves.
    assert state.reset_seconds == 5


@pytest.mark.asyncio
async def test_window_state_empty_window_is_zeroed():
    state = await RateLimitUsageReader(_FakeRedis({})).window_state(
        "k", window_seconds=60, now=1_000
    )
    assert state.used == 0
    assert state.reset_seconds == 0


@pytest.mark.parametrize(
    ("member", "expected"),
    [
        ("uuid-1|5", 5),
        (b"uuid-1|7", 7),
        ("uuid-1", 1),
        ("uuid-1|x", 1),
        ("", 1),
        ("a|b|12", 12),
    ],
)
def test_parse_member_count(member, expected):
    assert parse_member_count(member) == expected


def test_reader_from_config_without_redis_is_none():
    cfg = SimpleNamespace(gateway_rate_limit=_cfg(redis=None))
    assert reader_from_config(cfg) is None


def test_reader_exposes_its_client():
    """The property hands back exactly the injected client: long-lived
    consumers (the usage flusher) SCAN through the same pool they read with."""
    client = _FakeRedis({})
    assert RateLimitUsageReader(client).client is client


def test_reader_from_config_dials_gateway_service():
    redis_cfg = GatewayRateLimitRedis(
        service_name="rate-limit-redis",
        service_port=6380,
        username="svc",
        password="pw",
        timeout=2500,
        database=3,
    )
    cfg = SimpleNamespace(gateway_rate_limit=_cfg(redis=redis_cfg))
    client = reader_from_config(cfg)._client
    # redis.asyncio.Redis connects lazily; its pool carries the dial plan.
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["host"] == "rate-limit-redis"
    assert kwargs["port"] == 6380
    assert kwargs["username"] == "svc"
    assert kwargs["password"] == "pw"
    assert kwargs["db"] == 3
    assert kwargs["socket_timeout"] == 2.5
    assert kwargs["socket_connect_timeout"] == 2.5
    assert kwargs["decode_responses"] is False


@pytest.mark.asyncio
async def test_specs_and_reader_round_trip():
    """The reader consumes exactly the keys expected_windows derives: every
    spec's totals land on its own key and nowhere else."""
    windows = expected_windows(
        _cfg(),
        6,
        None,
        _UserStub(weekly_token_limit=100_000, monthly_token_limit=200_000),
    )
    seed = {}
    expected_used = {}
    for i, spec in enumerate(windows):
        in_window, outside = 2 * (i + 1), 3 * (i + 1)
        # Below the spec's own exclusive lower bound, so it stays outside
        # however wide the window is (the weekly one reaches far back).
        lower = 1_000 - (spec.window_seconds or 200)
        seed[spec.redis_key] = [
            (lower - 1, f"stale-{i}|100"),
            (990, f"a-{i}|{in_window}"),
            (999, f"b-{i}|{outside}"),
        ]
        expected_used[spec.redis_key] = in_window + outside
    reader = RateLimitUsageReader(_FakeRedis(seed))
    for spec in windows:
        # Calendar windows carry no fixed length; the endpoint passes the
        # elapsed period, any value bounding the read below `now` works here.
        state = await reader.window_state(
            spec.redis_key,
            window_seconds=spec.window_seconds or 200,
            now=1_000,
        )
        assert state.used == expected_used[spec.redis_key], spec.redis_key


@pytest.mark.asyncio
async def test_read_window_rolling_matches_window_state(monkeypatch):
    def _forbid_calendar(*args, **kwargs):
        raise AssertionError("rolling windows must not consult calendar math")

    monkeypatch.setattr(rate_limit_usage, "calendar_elapsed_seconds", _forbid_calendar)
    monkeypatch.setattr(rate_limit_usage, "calendar_window_seconds", _forbid_calendar)
    now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
    now_ts = int(now.timestamp())
    client = _FakeRedis({"k": [(now_ts - 30, "a|2"), (now_ts - 5, "b|3")]})
    state = await RateLimitUsageReader(client).read_window(
        _spec(), now=now, timezone_name="UTC"
    )
    assert state.used == 5
    # Rolling reset is member aging: the oldest in-window member leaves 30s
    # from now, nothing calendar-derived.
    assert state.reset_seconds == 30


@pytest.mark.asyncio
async def test_read_window_calendar_overrides_reset():
    now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
    now_ts = int(now.timestamp())
    client = _FakeRedis(
        {
            "k": [
                # Older than the elapsed day yet inside the full 24h before
                # it: only the elapsed-period bound keeps it out of the read.
                (now_ts - 50_000, "before-day-start|7"),
                (now_ts - 1_000, "a|2"),
                (now_ts - 5, "b|3"),
            ]
        }
    )
    state = await RateLimitUsageReader(client).read_window(
        _spec(kind="token_calendar", period="each_day", window_seconds=None),
        now=now,
        timezone_name="UTC",
    )
    # The read bound is the elapsed period (12h at noon), not the full day.
    assert state.used == 5
    # Member aging would report 42_200 (elapsed minus the oldest member's
    # age); a calendar quota drops at the boundary instead.
    assert state.reset_seconds == 43_200


@pytest.mark.asyncio
async def test_read_window_rejects_naive_now():
    reader = RateLimitUsageReader(_FakeRedis({}))
    with pytest.raises(ValueError):
        await reader.read_window(
            _spec(), now=datetime(2026, 11, 19, 12, 0, 0), timezone_name="UTC"
        )


def test_user_limit_combo_is_the_key_segment():
    """The read side filters user windows on this segment: it must stay
    identical to what ``user_limit_windows`` renders into the key."""
    assert user_limit_combo(6) == "user-6"
    windows = expected_windows(
        _cfg(),
        6,
        None,
        _UserStub(weekly_token_limit=100_000, monthly_token_limit=200_000),
    )
    user_windows = [
        w for w in windows if w.redis_key.split("|")[1] == user_limit_combo(6)
    ]
    assert [(w.kind, w.period, w.limit) for w in user_windows] == [
        ("query", "60s", 10),
        ("token_rolling", "60s", 60_000),
        ("token_rolling", "604800s", 100_000),
        ("token_calendar", "each_month", 200_000),
    ]


@pytest.mark.asyncio
async def test_read_windows_returns_every_state():
    now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
    now_ts = int(now.timestamp())
    specs = [
        _spec(redis_key="a"),
        _spec(redis_key="b", kind="token_rolling"),
    ]
    reader = RateLimitUsageReader(
        _FakeRedis(
            {
                "a": [(now_ts - 1, "m|2")],
                "b": [(now_ts - 10, "n|4")],
            }
        )
    )
    states, error = await read_windows(reader, specs, now=now, timezone_name="UTC")
    assert error is None
    # Rolling reset is member aging: a member 1s (10s) old leaves in 59s (50s).
    assert [(s.used, s.reset_seconds) for s in states] == [(2, 59), (4, 50)]


@pytest.mark.asyncio
async def test_read_windows_discards_everything_on_any_failure():
    """A bad deployment timezone fails the calendar windows' boundary math;
    the batch is discarded whole — the rolling read that succeeded is
    thrown away with it."""
    now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
    now_ts = int(now.timestamp())
    specs = [
        _spec(redis_key="a"),
        _spec(
            redis_key="b",
            kind="token_calendar",
            period="each_day",
            window_seconds=None,
        ),
    ]
    reader = RateLimitUsageReader(
        _FakeRedis({"a": [(now_ts - 1, "m|2")], "b": [(now_ts - 1, "n|2")]})
    )
    states, error = await read_windows(
        reader, specs, now=now, timezone_name="Mars/Olympus"
    )
    assert error == "ZoneInfoNotFoundError"
    assert states == [None, None]
