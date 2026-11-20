"""The ``GET /usage/rate-limits`` endpoint: scoping, modes and fail-open reads.

Offline by construction: the reader is injected through the module-level
``_reader_factory`` indirection, and the session / request are fakes shaped
like the ones ``tests/routes/test_api_keys.py`` uses to call route handlers
directly.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from redis.exceptions import RedisError
from zoneinfo import ZoneInfoNotFoundError

from gpustack.api.exceptions import (
    BadRequestException,
    InvalidException,
    NotFoundException,
)
from gpustack.config.config import GatewayRateLimitConfig, GatewayRateLimitRedis
from gpustack.gateway.rate_limit import rate_limit_plugin_name
from gpustack.gateway.rate_limit_usage import RateLimitUsageReader, RateLimitWindowState
from gpustack.routes import rate_limits
from gpustack.schemas.api_keys import ApiKey
from gpustack.schemas.rate_limits import RateLimitResetCreate
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


def _user(
    id,
    *,
    is_admin=False,
    weekly=None,
    monthly=None,
    rpm=None,
    tpm=None,
    model_concurrency_limits=None,
) -> User:
    return User(
        id=id,
        name=f"user-{id}",
        is_admin=is_admin,
        rpm_limit=rpm,
        tpm_limit=tpm,
        weekly_token_limit=weekly,
        monthly_token_limit=monthly,
        model_concurrency_limits=model_concurrency_limits,
    )


def _key(id, *, access_key="ak", daily=None) -> ApiKey:
    stamp = FIXED_NOW.replace(tzinfo=None)
    return ApiKey(
        id=id,
        name=f"key-{id}",
        user_id=7,
        access_key=access_key,
        hashed_secret_key=f"hash-{id}",
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
    listing through ``session.exec``, and reset-quota reads through the
    ``gateway_rate_limit_resets`` statements. Writes are recorded, not
    persisted."""

    def __init__(self, users=(), keys=(), resets=()):
        self._users = {u.id: u for u in users}
        self._keys = list(keys)
        self._resets = sorted(resets, key=lambda r: r.reset_at)
        self.get_calls = []
        self.statements = []
        self.added = []
        # What ActiveRecordMixin's event publishing touches on save.
        self.sync_session = SimpleNamespace(
            in_transaction=lambda: True,
            autobegin=True,
            begin=lambda: None,
        )
        self.info = {}
        self.is_active = True
        self.dirty = set()

    async def flush(self, targets=None):
        pass

    async def rollback(self):
        pass

    async def get(self, cls, id, **kwargs):
        self.get_calls.append(id)
        return self._users.get(id)

    async def exec(self, statement):
        self.statements.append(statement)
        text = self._compiled(statement)
        if "gateway_rate_limit_resets" in text:
            return SimpleNamespace(all=lambda: self._matching_resets(text))
        return SimpleNamespace(all=lambda: list(self._keys))

    @staticmethod
    def _compiled(statement) -> str:
        """The statement with literals inlined, so the fake can read the
        kind / user filters the real database would apply."""
        try:
            return str(statement.compile(compile_kwargs={"literal_binds": True}))
        except Exception:
            return str(statement)

    def _matching_resets(self, text):
        """Rows the reset-status query would fetch: the kind and user
        filters read off the compiled statement, the self-service filter
        (an admin's press never counts) mirrored in Python."""
        matched = [
            row
            for row in self._resets
            if f"kind = '{row.kind}'" in text
            and f"user_id = {row.user_id}" in text
            and (row.reset_by_user_id is None or row.reset_by_user_id == row.user_id)
        ]
        matched.sort(key=lambda r: r.reset_at)
        return matched

    def add(self, obj):
        self.added.append(obj)
        self._resets.append(obj)
        self._resets.sort(key=lambda r: r.reset_at)

    async def commit(self):
        pass

    async def refresh(self, obj):
        pass


class FakeReader:
    """Records the specs it is asked to read; returns the seeded state for
    keys that have one (a default otherwise). With ``error`` set, every
    key WITHOUT a seeded state raises — which is what makes a mixed batch:
    some reads succeed, some fail. Concurrency counts are seeded the same
    way via ``concurrency`` (default 0), with ``concurrency_error`` failing
    the whole batch."""

    def __init__(
        self,
        states=None,
        error=None,
        concurrency=None,
        concurrency_error=None,
    ):
        self.specs = []
        self._states = states or {}
        self._error = error
        self.concurrency_keys = []
        self._concurrency = concurrency or {}
        self._concurrency_error = concurrency_error
        self.deleted = []
        self.client = SimpleNamespace(delete=self._delete)

    async def _delete(self, *keys):
        self.deleted.extend(keys)
        return len(keys)

    async def window_states(self, specs, *, now, timezone_name):
        self.specs.extend(specs)
        states = []
        for spec in specs:
            if spec.redis_key in self._states:
                states.append(self._states[spec.redis_key])
            elif self._error is not None:
                raise self._error
            else:
                states.append(RateLimitWindowState(used=3, reset_seconds=10))
        return states

    async def concurrency_counts(self, keys):
        self.concurrency_keys.extend(keys)
        if self._concurrency_error is not None:
            raise self._concurrency_error
        return [self._concurrency.get(key, 0) for key in keys]


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
    """Queues ZRANGEBYSCORE commands and replays them on ``execute`` — the
    slice of redis-py's pipeline the batched reader uses."""

    def __init__(self, client):
        self._client = client
        self._commands = []

    def zrangebyscore(self, key, lo, hi, withscores=False):
        self._commands.append((key, lo, hi, withscores))
        return self

    async def execute(self, raise_on_error=True):
        results = []
        for key, lo, hi, withscores in self._commands:
            try:
                results.append(
                    self._client._zrangebyscore_sync(key, lo, hi, withscores)
                )
            except Exception as exc:
                if raise_on_error:
                    raise
                results.append(exc)
        return results


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
    session = FakeSession(users=[user], keys=[_key(1, access_key="ak-1")])
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
    # The user block is the user limit combo verbatim: per-minute caps and
    # monthly inherit the deployment defaults, the weekly budget is set.
    assert _shapes(response.user.windows) == [
        ("query", "60s", 10),
        ("token_rolling", "60s", 60_000),
        ("token_rolling", "604800s", 100_000),
        ("token_calendar", "each_month", 200_000),
    ]
    assert response.api_keys[0].source == "user+key"
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
        ("query", "60s", 10),
        ("token_rolling", "60s", 60_000),
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
async def test_model_concurrency_reports_overrides_with_running_counts(
    frozen_now, monkeypatch
):
    """The per-model block mirrors the user's ``model_concurrency_limits``
    with live slot counts; the keys read are the plugin's concurrency
    layout under the user limit combo."""
    cfg = _redis_cfg()
    user = _user(7, model_concurrency_limits={"glm-5.3-flash": 4, "free": 0})
    session = FakeSession(users=[user], keys=[])
    reader = FakeReader(
        concurrency={
            "gpustack-rate-limit|user-7|consumer=gpustack-7|c:glm-5.3-flash": 2,
            "gpustack-rate-limit|user-7|consumer=gpustack-7|c:free": 0,
        }
    )
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(cfg), user_id=None
    )

    assert [(m.model, m.limit, m.running) for m in response.user.model_concurrency] == [
        ("free", 0, 0),
        ("glm-5.3-flash", 4, 2),
    ]
    assert sorted(reader.concurrency_keys) == [
        "gpustack-rate-limit|user-7|consumer=gpustack-7|c:free",
        "gpustack-rate-limit|user-7|consumer=gpustack-7|c:glm-5.3-flash",
    ]
    assert response.usage_error is None


@pytest.mark.asyncio
async def test_model_concurrency_admin_and_no_overrides_get_nothing(
    frozen_now, monkeypatch
):
    """Admins are exempt (nothing enforced), and a user without overrides
    has no itemized per-model caps: both get an empty block and no
    concurrency reads at all."""
    cfg = _redis_cfg()
    admin = _user(1, is_admin=True, model_concurrency_limits={"glm": 4})
    plain = _user(7)
    session = FakeSession(users=[admin, plain])
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    admin_response = await rate_limits.get_rate_limits(
        session=session, user=admin, ctx=_ctx(10), request=_request(cfg), user_id=None
    )
    plain_response = await rate_limits.get_rate_limits(
        session=session, user=plain, ctx=_ctx(10), request=_request(cfg), user_id=None
    )

    assert admin_response.user.model_concurrency == []
    assert plain_response.user.model_concurrency == []
    assert reader.concurrency_keys == []


@pytest.mark.asyncio
async def test_model_concurrency_failure_fails_open(frozen_now, monkeypatch):
    """A failed concurrency read blanks the running counts instead of
    mixing fresh and stale numbers, and surfaces the same usage_error
    channel the window reads use."""
    cfg = _redis_cfg()
    user = _user(7, model_concurrency_limits={"glm-5.3-flash": 4})
    session = FakeSession(users=[user], keys=[])
    reader = FakeReader(concurrency_error=ConnectionError("down"))
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(cfg), user_id=None
    )

    assert response.usage_error == "ConnectionError"
    assert [(m.model, m.limit, m.running) for m in response.user.model_concurrency] == [
        ("glm-5.3-flash", 4, None)
    ]


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
    assert _shapes(response.user.windows) == [
        ("query", "60s", 10),
        ("token_rolling", "60s", 60_000),
        ("token_rolling", "604800s", 111),
        ("token_calendar", "each_month", 20_000_000),
    ]
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
    session = FakeSession(users=[user], keys=[_key(1, access_key="ak-1")])

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(_local_cfg())
    )

    assert response.mode == "local"
    assert response.usage_error is None
    # Effective user windows still list their limits: minute caps and the
    # monthly quota inherit the deployment defaults, the weekly budget is
    # the user's own.
    assert [w.period for w in response.user.windows] == [
        "60s",
        "60s",
        "604800s",
        "each_month",
    ]
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
    session = FakeSession(users=[user], keys=[_key(1, access_key="ak-1")])
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
    no combo for them); every live key binds its own day quota on top of the
    user's shared combo."""
    cfg = _redis_cfg()
    user = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(
        users=[user],
        keys=[
            # Legacy deployment token: it must not appear in the per-key view.
            _key(1, access_key=""),
            _key(2, access_key="ak-explicit", daily=500_000),
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
    # The explicit daily limit wins over the deployment default.
    assert by_id[2].source == "user+key"
    assert _shapes(by_id[2].windows) == [
        ("token_calendar", "each_day", 500_000),
    ]
    assert by_id[3].source == "user+key"
    # An unset daily limit inherits the deployment default.
    assert _shapes(by_id[3].windows) == [
        ("token_calendar", "each_day", 2_000_000),
    ]


@pytest.mark.asyncio
async def test_calendar_day_read_is_bounded_by_elapsed_period(frozen_now, monkeypatch):
    """Round trip through the real reader: the daily counter is bounded by
    the elapsed part of the day, so a member older than the day start but
    inside the last 24h stays out of ``used``."""
    now_ts = int(FIXED_NOW.timestamp())
    daily_key = (
        f"{rate_limit_plugin_name}|key-3|consumer=ak-shared.gpustack-7|t:each_day"
    )
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
    # The user block rides the user combo: minute caps and the monthly
    # quota inherit the deployment defaults (unseeded buckets read zero).
    assert _shapes(response.user.windows) == [
        ("query", "60s", 10),
        ("token_rolling", "60s", 60_000),
        ("token_calendar", "each_month", 20_000_000),
    ]
    user_monthly = next(w for w in response.user.windows if w.period == "each_month")
    assert user_monthly.used == 0
    assert user_monthly.reset_seconds is not None


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


@pytest.mark.asyncio
async def test_reader_is_cached_on_app_state_across_requests(frozen_now, monkeypatch):
    """The reader is built once per process — the config it dials is
    process-static — and reused: two requests sharing an app.state hit the
    factory exactly once."""
    cfg = _redis_cfg()
    user = _user(7)
    session = FakeSession(users=[user], keys=[_key(1, access_key="ak-1")])
    request = _request(cfg)
    calls = []

    def factory(cfg):
        calls.append(cfg)
        return FakeReader()

    monkeypatch.setattr(rate_limits, "_reader_factory", factory)

    await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=request
    )
    await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=request
    )

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_mixed_reader_failure_discards_the_successful_reads_too(
    frozen_now, monkeypatch
):
    """A batch with both succeeded and failed reads is discarded whole:
    even the window whose read DID succeed is blanked, so the page never
    mixes fresh counters with stale ones."""
    cfg = _redis_cfg()
    user = _user(7)
    session = FakeSession(users=[user], keys=[_key(3, access_key="ak-shared")])
    daily_key = f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|t:each_day"
    reader = FakeReader(
        states={daily_key: RateLimitWindowState(used=5, reset_seconds=43_200)},
        error=TimeoutError("read timed out"),
    )
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(cfg)
    )

    assert response.usage_error == "TimeoutError"
    assert reader.specs  # the failing reads were attempted too
    for window in _all_windows(response):
        assert window.used is None
        assert window.reset_seconds is None


@pytest.mark.parametrize(
    "error_type", [RedisError, TimeoutError, ZoneInfoNotFoundError]
)
@pytest.mark.asyncio
async def test_reader_failure_surfaces_only_the_class_name(
    frozen_now, monkeypatch, error_type
):
    """Whatever the failure — connection error, timeout, or a bad
    deployment timezone blowing up the calendar math — only the exception's
    class name reaches the payload, never its message."""
    cfg = _redis_cfg()
    user = _user(7, weekly=100_000)
    session = FakeSession(users=[user], keys=[_key(1, access_key="ak-1")])
    reader = FakeReader(error=error_type("dial tcp: internal-host:6379"))
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(cfg)
    )

    assert response.usage_error == error_type.__name__
    assert "internal-host" not in response.usage_error
    for window in _all_windows(response):
        assert window.used is None
        assert window.reset_seconds is None


def _reset(user_id, kind, at, by=None):
    """A recorded reset row, aware UTC as UTCDateTime returns it."""
    return SimpleNamespace(user_id=user_id, kind=kind, reset_at=at, reset_by_user_id=by)


@pytest.mark.asyncio
async def test_get_reports_floating_reset_quota(frozen_now, monkeypatch):
    """The response carries per-kind reset state: two presses per floating
    window, the already-used ones counted, the disabled button carrying the
    instant the oldest counted press ages out."""
    cfg = _redis_cfg()
    user = _user(7, weekly=100_000, monthly=200_000)
    resets = [
        _reset(7, "weekly", FIXED_NOW - timedelta(days=2)),
        _reset(7, "weekly", FIXED_NOW - timedelta(days=1)),
        # An admin reset on the user's behalf never consumes their quota.
        _reset(7, "monthly", FIXED_NOW - timedelta(days=3), by=1),
    ]
    session = FakeSession(users=[user], keys=[], resets=resets)
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(cfg)
    )

    weekly = response.user.resets["weekly"]
    assert weekly.quota == 2
    assert weekly.available == 0
    # The oldest self press was two days ago: it frees up in five.
    assert weekly.next_available_at == FIXED_NOW + timedelta(days=5)
    monthly = response.user.resets["monthly"]
    assert monthly.available == 2
    assert monthly.next_available_at is None


@pytest.mark.asyncio
async def test_reset_records_row_and_clears_counters(frozen_now, monkeypatch):
    """A press records the audit row (with the reason note), deletes the
    weekly counters of both the user combo and the default-plan fallback,
    and answers with the decremented quota."""
    cfg = _redis_cfg()
    user = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(users=[user], keys=[])
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    status = await rate_limits.reset_rate_limit(
        session=session,
        user=user,
        ctx=_ctx(10),
        request=_request(cfg),
        body=RateLimitResetCreate(kind="weekly", description="backfill job"),
    )

    assert status.available == 1
    assert status.next_available_at is None
    (row,) = session.added
    assert row.user_id == 7 and row.kind == "weekly"
    assert row.description == "backfill job"
    # The press is a self-service one: it counts against the quota.
    assert row.reset_by_user_id is None
    # The published weekly counter went away (the default plan publishes
    # no weekly window; its day / month counters are untouched kinds).
    assert reader.deleted == [
        "gpustack-rate-limit|user-7|consumer=gpustack-7|t:604800s"
    ]


@pytest.mark.asyncio
async def test_reset_quota_exhausted_rejects(frozen_now, monkeypatch):
    """Two presses inside the floating window exhaust it: the third is
    rejected and nothing is recorded or deleted."""
    cfg = _redis_cfg()
    user = _user(7, weekly=100_000, monthly=200_000)
    resets = [
        _reset(7, "weekly", FIXED_NOW - timedelta(days=6)),
        _reset(7, "weekly", FIXED_NOW - timedelta(days=1)),
    ]
    session = FakeSession(users=[user], keys=[], resets=resets)
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    with pytest.raises(BadRequestException):
        await rate_limits.reset_rate_limit(
            session=session,
            user=user,
            ctx=_ctx(10),
            request=_request(cfg),
            body=RateLimitResetCreate(kind="weekly", description="again"),
        )
    assert session.added == [] and reader.deleted == []


@pytest.mark.asyncio
async def test_admin_reset_bypasses_quota(frozen_now, monkeypatch):
    """A platform-wide admin resets on the user's behalf without touching
    the user's own allowance: the row is recorded with the acting admin,
    the quota state stays untouched."""
    cfg = _redis_cfg()
    admin = _user(1, is_admin=True, weekly=50_000, monthly=90_000)
    target = _user(7, weekly=100_000, monthly=200_000)
    resets = [
        _reset(7, "weekly", FIXED_NOW - timedelta(days=1)),
        _reset(7, "weekly", FIXED_NOW - timedelta(days=2)),
    ]
    session = FakeSession(users=[admin, target], keys=[], resets=resets)
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    status = await rate_limits.reset_rate_limit(
        session=session,
        user=admin,
        ctx=_ctx(None),
        request=_request(cfg),
        body=RateLimitResetCreate(kind="weekly", description="approved", user_id=7),
    )

    (row,) = session.added
    assert row.user_id == 7 and row.reset_by_user_id == 1
    # The admin's own press does not consume the user's quota.
    assert status.available == 0
    assert status.next_available_at is not None


@pytest.mark.asyncio
async def test_reset_validations(frozen_now, monkeypatch):
    """Unknown kinds, blank notes and window kinds the user does not have
    are rejected before anything is written or deleted."""
    cfg = _redis_cfg()
    user = _user(7, monthly=200_000)
    session = FakeSession(users=[user], keys=[])
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    with pytest.raises(InvalidException):
        await rate_limits.reset_rate_limit(
            session=session,
            user=user,
            ctx=_ctx(10),
            request=_request(cfg),
            body=RateLimitResetCreate(kind="daily", description="x"),
        )
    with pytest.raises(InvalidException):
        await rate_limits.reset_rate_limit(
            session=session,
            user=user,
            ctx=_ctx(10),
            request=_request(cfg),
            body=RateLimitResetCreate(kind="weekly", description="   "),
        )
    # No weekly budget is configured for this user at all.
    with pytest.raises(InvalidException):
        await rate_limits.reset_rate_limit(
            session=session,
            user=user,
            ctx=_ctx(10),
            request=_request(cfg),
            body=RateLimitResetCreate(kind="weekly", description="x"),
        )
    assert session.added == [] and reader.deleted == []
