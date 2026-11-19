"""The ``GET /usage/rate-limits`` endpoint: scoping, modes and fail-open reads.

Offline by construction: the reader is injected through the module-level
``_reader_factory`` indirection, and the session / request are fakes shaped
like the ones ``tests/routes/test_api_keys.py`` uses to call route handlers
directly.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from gpustack.api.exceptions import NotFoundException
from gpustack.config.config import GatewayRateLimitConfig, GatewayRateLimitRedis
from gpustack.gateway.rate_limit import rate_limit_plugin_name
from gpustack.gateway.rate_limit_usage import RateLimitUsageReader, RateLimitWindowState
from gpustack.routes import rate_limits
from gpustack.schemas.api_keys import ApiKey
from gpustack.schemas.users import User

# A fixed instant with a 12h elapsed UTC day, so calendar boundaries are
# exact regardless of when the suite runs.
FIXED_NOW = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)


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


def _redis_cfg() -> SimpleNamespace:
    """Top-level config stand-in: the endpoint only reads
    ``gateway_rate_limit`` off it."""
    return SimpleNamespace(gateway_rate_limit=_cfg())


def _local_cfg() -> SimpleNamespace:
    return SimpleNamespace(gateway_rate_limit=_cfg(redis=None))


def _request(cfg) -> SimpleNamespace:
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(server_config=cfg))
    )


def _ctx(current_principal_id) -> SimpleNamespace:
    """Minimal TenantContext stand-in: the platform-wide gate reads only
    ``current_principal_id`` (fake shape mirrors
    tests/routes/test_api_keys.py's context stub)."""
    return SimpleNamespace(current_principal_id=current_principal_id)


def _user(id, *, is_admin=False, weekly=None, monthly=None) -> User:
    return User(
        id=id,
        name=f"user-{id}",
        is_admin=is_admin,
        weekly_token_limit=weekly,
        monthly_token_limit=monthly,
    )


def _key(id, *, access_key="ak", rpm_limit=None, tpm_limit=None, daily=None) -> ApiKey:
    stamp = FIXED_NOW.replace(tzinfo=None)
    return ApiKey(
        id=id,
        name=f"key-{id}",
        user_id=7,
        access_key=access_key,
        hashed_secret_key=f"hash-{id}",
        rpm_limit=rpm_limit,
        tpm_limit=tpm_limit,
        daily_token_limit=daily,
        created_at=stamp,
        updated_at=stamp,
    )


def _shapes(windows):
    """(kind, period, limit) per window — the identity the UI renders."""
    return [(w.kind, w.period, w.limit) for w in windows]


class FakeSession:
    """Stand-in for the async session the endpoint uses: admin user loads
    resolve through ``session.get`` (what ``one_by_id`` calls), the api-keys
    listing through ``session.exec``."""

    def __init__(self, users=(), keys=()):
        self._users = {u.id: u for u in users}
        self._keys = list(keys)
        self.get_calls = []
        self.statements = []

    async def get(self, cls, id, **kwargs):
        self.get_calls.append(id)
        return self._users.get(id)

    async def exec(self, statement):
        self.statements.append(statement)
        return SimpleNamespace(all=lambda: list(self._keys))


class FakeReader:
    """Records the specs it is asked to read; returns a fixed state per key
    or raises, to drive the failure path."""

    def __init__(self, states=None, error=None):
        self.specs = []
        self._states = states or {}
        self._error = error

    async def read_window(self, spec, *, now, timezone_name):
        self.specs.append(spec)
        if self._error is not None:
            raise self._error
        return self._states.get(
            spec.redis_key, RateLimitWindowState(used=3, reset_seconds=10)
        )


class FakeRedis:
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

    async def zrangebyscore(self, key, lo, hi, withscores=False):
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


class FrozenDateTime(datetime):
    """``datetime.now()`` pinned to FIXED_NOW so calendar boundary math is
    deterministic; injected over the route module's ``datetime``."""

    @classmethod
    def now(cls, tz=None):
        if tz is not None:
            return FIXED_NOW.astimezone(tz)
        return FIXED_NOW.replace(tzinfo=None)


@pytest.fixture
def frozen_now(monkeypatch):
    monkeypatch.setattr(rate_limits, "datetime", FrozenDateTime)
    return FIXED_NOW


def _all_windows(response):
    return response.user.windows + [w for key in response.api_keys for w in key.windows]


@pytest.mark.asyncio
async def test_non_admin_gets_own_data_regardless_of_user_id(frozen_now, monkeypatch):
    """A foreign ``user_id`` is silently ignored for non-admins: the
    response is always the caller's own picture (and no admin expansion
    lookup ever runs)."""
    cfg = _redis_cfg()
    user = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(users=[user], keys=[_key(1, access_key="ak-1", rpm_limit=30)])
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(cfg), user_id=99
    )

    assert session.get_calls == []
    assert response.mode == "redis"
    assert response.generated_at == FIXED_NOW
    assert response.user.weekly_limit == 100_000
    assert response.user.monthly_limit == 200_000
    # The user block carries only the user-budget combo, not the defaults.
    assert _shapes(response.user.windows) == [
        ("token_rolling", "604800s", 100_000),
        ("token_calendar", "each_month", 200_000),
    ]
    assert response.api_keys[0].source == "key"
    # The listing is scoped to the target's live keys.
    sql = str(session.statements[0])
    assert "api_keys.user_id" in sql
    assert "api_keys.deleted_at IS NULL" in sql


@pytest.mark.asyncio
async def test_admin_reads_another_user(frozen_now, monkeypatch):
    """A platform-wide admin (admin, no pinned Org) may read another
    user's picture."""
    cfg = _redis_cfg()
    admin = _user(1, is_admin=True)
    target = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(users=[admin, target], keys=[_key(2, access_key="ak-2")])
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=admin, ctx=_ctx(None), request=_request(cfg), user_id=7
    )

    assert session.get_calls == [7]
    assert response.user.weekly_limit == 100_000
    assert _shapes(response.user.windows) == [
        ("token_rolling", "604800s", 100_000),
        ("token_calendar", "each_month", 200_000),
    ]
    assert [k.id for k in response.api_keys] == [2]
    # Every read ran for the target user's combos.
    assert all("gpustack-7" in spec.redis_key for spec in reader.specs)


@pytest.mark.asyncio
async def test_admin_unknown_user_is_404(frozen_now):
    admin = _user(1, is_admin=True)
    session = FakeSession(users=[admin])
    with pytest.raises(NotFoundException) as exc:
        await rate_limits.get_rate_limits(
            session=session,
            user=admin,
            ctx=_ctx(None),
            request=_request(_redis_cfg()),
            user_id=999,
        )
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_org_pinned_admin_foreign_user_id_degrades_to_self(
    frozen_now, monkeypatch
):
    """An Org-pinned admin is tenant-scoped like anyone else: a foreign
    ``user_id`` quietly degrades to the caller's own picture, and no
    cross-user lookup ever runs."""
    cfg = _redis_cfg()
    admin = _user(1, is_admin=True, weekly=111)
    foreign = _user(7, weekly=222)
    session = FakeSession(users=[admin, foreign], keys=[])
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=admin, ctx=_ctx(10), request=_request(cfg), user_id=7
    )

    assert session.get_calls == []
    assert response.user.weekly_limit == 111
    assert _shapes(response.user.windows) == [("token_rolling", "604800s", 111)]
    assert response.api_keys == []


@pytest.mark.asyncio
async def test_local_mode_omits_counters_without_touching_the_reader(
    frozen_now, monkeypatch
):
    """Local mode has no counters: windows still carry their limits, but
    ``used`` / ``reset_seconds`` drop off the wire and no reader is built."""

    def _forbid(cfg):
        raise AssertionError("local mode must not build a reader")

    monkeypatch.setattr(rate_limits, "_reader_factory", _forbid)
    user = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(users=[user], keys=[_key(1, access_key="ak-1", rpm_limit=30)])

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(_local_cfg())
    )

    assert response.mode == "local"
    assert response.usage_error is None
    # Only the user-budget combo shows; the defaults are not user-attributable.
    assert [w.period for w in response.user.windows] == ["604800s", "each_month"]
    for window in _all_windows(response):
        assert window.used is None
        assert window.reset_seconds is None
        assert window.limit is not None
    dumped = response.model_dump(exclude_none=True)
    assert "used" not in dumped["user"]["windows"][0]
    assert "used" not in dumped["api_keys"][0]["windows"][0]


@pytest.mark.asyncio
async def test_reader_failure_blanks_all_counters(frozen_now, monkeypatch):
    """Any failed read blanks every counter — partial results are never
    mixed with fresh ones — while the limits survive, and only the
    exception's class name surfaces (messages can carry hostnames)."""
    cfg = _redis_cfg()
    user = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(users=[user], keys=[_key(1, access_key="ak-1", rpm_limit=30)])
    reader = FakeReader(error=RuntimeError("dial tcp: redis-host:6379"))
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(cfg)
    )

    assert response.mode == "redis"
    assert response.usage_error == "RuntimeError"
    assert "redis-host" not in response.usage_error
    assert reader.specs  # the reads were attempted
    for window in _all_windows(response):
        assert window.used is None
        assert window.reset_seconds is None
        assert window.limit is not None


@pytest.mark.asyncio
async def test_legacy_key_hidden_and_sources(frozen_now, monkeypatch):
    """Deployment-token rows carry no per-key view (the reconciler publishes
    no combo for them); explicit-limit keys bind their own combo, unlimited
    keys ride the fallback group with the owner's budgets."""
    cfg = _redis_cfg()
    user = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(
        users=[user],
        keys=[
            # Legacy deployment token: explicit limits on the row, but it
            # must not appear in the per-key view.
            _key(1, access_key="", rpm_limit=5),
            _key(2, access_key="ak-explicit", rpm_limit=30),
            _key(3, access_key="ak-shared"),
        ],
    )
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(cfg)
    )

    by_id = {k.id: k for k in response.api_keys}
    assert set(by_id) == {2, 3}
    assert by_id[2].source == "key"
    # The key combo folds in the deployment defaults and the owner budgets.
    assert _shapes(by_id[2].windows) == [
        ("query", "60s", 30),
        ("token_rolling", "60s", 60_000),
        ("token_rolling", "604800s", 100_000),
        ("token_calendar", "each_day", 2_000_000),
        ("token_calendar", "each_month", 200_000),
    ]
    assert by_id[3].source == "user+default"
    # The fallback group: default plan first, then the user budget combo.
    assert _shapes(by_id[3].windows) == [
        ("query", "60s", 10),
        ("token_rolling", "60s", 60_000),
        ("token_calendar", "each_day", 2_000_000),
        ("token_calendar", "each_month", 20_000_000),
        ("token_rolling", "604800s", 100_000),
        ("token_calendar", "each_month", 200_000),
    ]


@pytest.mark.asyncio
async def test_calendar_day_read_is_bounded_by_elapsed_period(frozen_now, monkeypatch):
    """Round trip through the real reader: the daily counter is bounded by
    the elapsed part of the day, so a member older than the day start but
    inside the last 24h stays out of ``used``."""
    now_ts = int(FIXED_NOW.timestamp())
    daily_key = f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|t:each_day"
    client = FakeRedis(
        {
            daily_key: [
                # 13.9h old: inside the last 24h, but before 00:00Z.
                (now_ts - 50_000, "before-day-start|7"),
                (now_ts - 1_000, "a|2"),
                (now_ts - 5, "b|3"),
            ]
        }
    )
    monkeypatch.setattr(
        rate_limits,
        "_reader_factory",
        lambda cfg: RateLimitUsageReader(client),
    )
    user = _user(7)
    session = FakeSession(users=[user], keys=[_key(3, access_key="ak-shared")])

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(_redis_cfg())
    )

    daily = next(w for w in response.api_keys[0].windows if w.period == "each_day")
    # Only the elapsed-period members count...
    assert daily.used == 5
    # ...and the reset is the calendar boundary, not member aging.
    assert daily.reset_seconds == 43_200


@pytest.mark.asyncio
async def test_disabled_mode_returns_empty_structures(frozen_now, monkeypatch):
    """With rate limiting off the section is empty — no reader, no DB
    reads — so the UI hides it."""

    def _forbid(cfg):
        raise AssertionError("disabled mode must not build a reader")

    monkeypatch.setattr(rate_limits, "_reader_factory", _forbid)
    user = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(users=[user], keys=[_key(1, access_key="ak-1")])

    response = await rate_limits.get_rate_limits(
        session=session,
        user=user,
        ctx=_ctx(10),
        request=_request(SimpleNamespace(gateway_rate_limit=_cfg(enabled=False))),
        user_id=999,
    )

    assert response.mode == "disabled"
    assert response.user.windows == []
    assert response.user.weekly_limit is None
    assert response.api_keys == []
    assert session.statements == []
    assert session.get_calls == []
