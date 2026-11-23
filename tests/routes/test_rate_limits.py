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
from gpustack.schemas.rate_limits import (
    FreeWindowUsage,
    RateLimitResetCreate,
)
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
        default_monthly_tokens=20_000_000,
    )
    fields.update(overrides)
    return GatewayRateLimitConfig(**fields)


def _redis_cfg(**overrides) -> SimpleNamespace:
    """Top-level config stand-in: the endpoint only reads
    ``gateway_rate_limit`` off it."""
    return SimpleNamespace(gateway_rate_limit=_cfg(**overrides))


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


def _shapes(windows):
    """(kind, period, limit) per window — the identity the UI renders."""
    return [(w.kind, w.period, w.limit) for w in windows]


class FakeSession:
    """Stand-in for the async session the endpoint uses: admin user loads
    resolve through ``session.get`` (what ``one_by_id`` calls), and
    reset-quota reads through the ``gateway_rate_limit_resets`` statements.
    Writes are recorded, not persisted."""

    def __init__(self, users=(), resets=(), models=()):
        self._users = {u.id: u for u in users}
        self._resets = sorted(resets, key=lambda r: r.reset_at)
        # Ready route names the non_admin_user_models view yields.
        self._models = list(models)
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
        if "gateway_rate_limit_settings" in text:
            # No settings row stored: the endpoint's read fails back to
            # the config file (and a disabled free window), which is what
            # a fresh fake session should look like.
            return SimpleNamespace(first=lambda: None)
        if "gateway_rate_limit_resets" in text:
            return SimpleNamespace(all=lambda: self._matching_resets(text))
        if "non_admin_user_models" in text:
            return SimpleNamespace(all=lambda: list(self._models))
        return SimpleNamespace(all=lambda: [])

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
        # The settings row (no ``kind``) never participates in the reset
        # queries the fake answers.
        if hasattr(obj, "kind"):
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
    return response.user.windows


@pytest.mark.asyncio
async def test_non_admin_gets_own_data_regardless_of_user_id(frozen_now, monkeypatch):
    """A foreign ``user_id`` is silently ignored for non-admins: the
    response is always the caller's own picture (and no admin expansion
    lookup ever runs)."""
    cfg = _redis_cfg()
    user = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(users=[user])
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
    # The deployment concurrency cap rides along for the UI's per-model
    # table; the per-key view is gone, so no api_keys statement ever runs.
    assert response.user.concurrency_default == 1
    assert session.statements
    # Every statement is either the settings read+seed, the reset-quota
    # read, the ready-models view, or the group-quota loader (membership
    # edges and principal kinds); no api_keys statement ever runs.
    assert all(
        "gateway_rate_limit_resets" in str(s)
        or "gateway_rate_limit_settings" in str(s)
        or "non_admin_user_models" in str(s)
        or "principal_memberships" in str(s)
        or "principals" in str(s)
        for s in session.statements
    )


@pytest.mark.asyncio
async def test_group_quotas_resolve_into_the_user_picture(frozen_now, monkeypatch):
    """The usage endpoint reports what the gateway enforces: the user's
    groups' quotas resolved per field — user explicit wins, else the
    smallest group value — not the raw user row."""
    cfg = _redis_cfg()
    # Explicit RPM wins over the groups; the other fields resolve from
    # them, monthly falling through to the deployment default.
    user = _user(7, rpm=50)
    session = FakeSession(users=[user])
    reader = FakeReader()
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    async def _loader(session, user_ids):
        return {
            7: [
                SimpleNamespace(
                    rpm_limit=2,
                    tpm_limit=1000,
                    weekly_token_limit=500_000,
                    monthly_token_limit=None,
                    model_concurrency_limits={"glm-5.3-flash": 1},
                ),
                SimpleNamespace(
                    rpm_limit=10,
                    tpm_limit=10_000,
                    weekly_token_limit=900_000,
                    monthly_token_limit=None,
                    model_concurrency_limits=None,
                ),
            ]
        }

    monkeypatch.setattr(rate_limits, "load_group_limit_snapshots", _loader)

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(cfg), user_id=None
    )

    assert response.user.rpm_limit == 50
    assert response.user.tpm_limit == 1000
    assert response.user.weekly_limit == 500_000
    # Nothing sets the monthly budget: the deployment default applies.
    assert response.user.monthly_limit == 20_000_000
    assert _shapes(response.user.windows) == [
        ("query", "60s", 50),
        ("token_rolling", "60s", 1000),
        ("token_rolling", "604800s", 500_000),
        ("token_calendar", "each_month", 20_000_000),
    ]


@pytest.mark.asyncio
async def test_admin_reads_another_user(frozen_now, monkeypatch):
    """A platform-wide admin (admin, no pinned Org) may read another
    user's picture."""
    cfg = _redis_cfg()
    admin = _user(1, is_admin=True)
    target = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(users=[admin, target])
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
    """The per-model block itemizes the ready models with live personal
    slot counts; the keys read are the plugin's concurrency layout under
    the user limit combo, and the busiest model leads."""
    cfg = _redis_cfg(default_concurrency=0)
    user = _user(7, model_concurrency_limits={"glm-5.3-flash": 4, "free": 0})
    session = FakeSession(users=[user], models=["free", "glm-5.3-flash", "other"])
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

    # "other" is ready but resolves to no cap (no override, deployment
    # default off), so it is not itemized; rows sort by running desc.
    assert [(m.model, m.limit, m.running) for m in response.user.model_concurrency] == [
        ("glm-5.3-flash", 4, 2),
        ("free", 0, 0),
    ]
    assert sorted(reader.concurrency_keys) == [
        "gpustack-rate-limit|user-7|consumer=gpustack-7|c:free",
        "gpustack-rate-limit|user-7|consumer=gpustack-7|c:glm-5.3-flash",
    ]
    assert response.usage_error is None


@pytest.mark.asyncio
async def test_model_concurrency_resolves_the_default_and_sorts_by_running(
    frozen_now, monkeypatch
):
    """A user without overrides gets every ready model at the deployment
    default cap, ordered busiest-first with the name as tiebreaker."""
    cfg = _redis_cfg(default_concurrency=2)
    user = _user(7)
    session = FakeSession(users=[user], models=["beta", "alpha", "gamma"])
    reader = FakeReader(
        concurrency={
            "gpustack-rate-limit|user-7|consumer=gpustack-7|c:alpha": 1,
            "gpustack-rate-limit|user-7|consumer=gpustack-7|c:beta": 0,
            "gpustack-rate-limit|user-7|consumer=gpustack-7|c:gamma": 2,
        }
    )
    monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: reader)

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(cfg), user_id=None
    )

    assert [(m.model, m.limit, m.running) for m in response.user.model_concurrency] == [
        ("gamma", 2, 2),
        ("alpha", 2, 1),
        ("beta", 2, 0),
    ]
    assert response.user.concurrency_default == 2


@pytest.mark.asyncio
async def test_model_concurrency_admin_and_capless_users_get_nothing(
    frozen_now, monkeypatch
):
    """Admins are exempt (no query, nothing enforced), and with the
    deployment cap off a user without overrides itemizes nothing even with
    ready models: both get an empty block and no concurrency reads."""
    cfg = _redis_cfg(default_concurrency=0)
    admin = _user(1, is_admin=True, model_concurrency_limits={"glm": 4})
    plain = _user(7)
    session = FakeSession(users=[admin, plain], models=["glm"])
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
    session = FakeSession(users=[user], models=["glm-5.3-flash"])
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
    session = FakeSession(users=[admin, foreign])
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
    session = FakeSession(users=[user])

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


@pytest.mark.asyncio
async def test_reader_failure_blanks_all_counters(frozen_now, monkeypatch):
    """Any failed read blanks every counter — partial results are never
    mixed with fresh ones — while the limits survive, and only the
    exception's class name surfaces (messages can carry hostnames)."""
    cfg = _redis_cfg()
    user = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(users=[user])
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
async def test_calendar_month_read_is_bounded_by_elapsed_period(
    frozen_now, monkeypatch
):
    """Round trip through the real reader: the monthly counter is bounded
    by the elapsed part of the calendar month, so a member older than the
    month start but inside the last 30 days stays out of ``used``."""
    now_ts = int(FIXED_NOW.timestamp())
    month_key = f"{rate_limit_plugin_name}|user-7|consumer=gpustack-7|t:each_month"
    client = FakeRedis(
        {
            month_key: [
                # ~29 days old: inside the last 30 days, but before Nov 1.
                (now_ts - 2_500_000, "before-month-start|7"),
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
    session = FakeSession(users=[user])

    response = await rate_limits.get_rate_limits(
        session=session, user=user, ctx=_ctx(10), request=_request(_redis_cfg())
    )

    monthly = next(w for w in response.user.windows if w.period == "each_month")
    # Only the elapsed-period members count...
    assert monthly.used == 5
    # ...and the reset is the calendar boundary, not member aging.
    assert monthly.reset_seconds == 993_600
    # The minute windows inherit the deployment defaults (unseeded buckets
    # read zero).
    assert _shapes(response.user.windows) == [
        ("query", "60s", 10),
        ("token_rolling", "60s", 60_000),
        ("token_calendar", "each_month", 20_000_000),
    ]
    user_query = next(w for w in response.user.windows if w.kind == "query")
    assert user_query.used == 0
    assert user_query.reset_seconds is not None


@pytest.mark.asyncio
async def test_disabled_mode_returns_empty_structures(frozen_now, monkeypatch):
    """With rate limiting off the section is empty — no reader, no DB
    reads — so the UI hides it."""

    def _forbid(cfg):
        raise AssertionError("disabled mode must not build a reader")

    monkeypatch.setattr(rate_limits, "_reader_factory", _forbid)
    user = _user(7, weekly=100_000, monthly=200_000)
    session = FakeSession(users=[user])

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
    assert session.statements == []
    assert session.get_calls == []


@pytest.mark.asyncio
async def test_reader_is_cached_on_app_state_across_requests(frozen_now, monkeypatch):
    """The reader is built once per process — the config it dials is
    process-static — and reused: two requests sharing an app.state hit the
    factory exactly once."""
    cfg = _redis_cfg()
    user = _user(7)
    session = FakeSession(users=[user])
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
    session = FakeSession(users=[user])
    month_key = (
        f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-7|t:each_month"
    )
    reader = FakeReader(
        states={month_key: RateLimitWindowState(used=5, reset_seconds=993_600)},
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
    session = FakeSession(users=[user])
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
    session = FakeSession(users=[user], resets=resets)
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
    session = FakeSession(users=[user])
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
    session = FakeSession(users=[user], resets=resets)
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
    session = FakeSession(users=[admin, target], resets=resets)
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
    session = FakeSession(users=[user])
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


class TestFreeWindowBadge:
    """The ``free`` block on the response: the admin-editable schedule and
    whether the moment of the read sits inside it."""

    @staticmethod
    def _patch(monkeypatch, free):
        async def _effective(session, cfg):
            return cfg.gateway_rate_limit, free

        monkeypatch.setattr(rate_limits, "effective_rate_limit", _effective)

    @pytest.mark.asyncio
    async def test_free_block_reports_the_schedule_and_activity(
        self, frozen_now, monkeypatch
    ):
        """FIXED_NOW is Wednesday 12:00 UTC: outside the seeded evening /
        weekend schedule, so ``active`` is false while the schedule itself
        still travels to the UI."""
        from gpustack.gateway.rate_limit_settings import FreeWindow

        cfg = _redis_cfg()
        user = _user(7)
        session = FakeSession(users=[user])
        monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: FakeReader())
        TestFreeWindowBadge._patch(
            monkeypatch,
            FreeWindow(enabled=True, start="15:00", end="06:00", days=["sat", "sun"]),
        )

        response = await rate_limits.get_rate_limits(
            session=session, user=user, ctx=_ctx(10), request=_request(cfg)
        )
        assert response.free == FreeWindowUsage(
            enabled=True,
            active=False,
            start="15:00",
            end="06:00",
            days=["sat", "sun"],
        )

    @pytest.mark.asyncio
    async def test_inside_the_window_the_badge_flips_active(
        self, frozen_now, monkeypatch
    ):
        """12:00 UTC Wednesday is 15:00 MSK -- inside the window when the
        deployment judges the schedule in Europe/Moscow. The UI uses this
        to badge a frozen counter instead of a broken read."""
        from gpustack.gateway.rate_limit_settings import FreeWindow

        cfg = _redis_cfg(timezone="Europe/Moscow")
        user = _user(7)
        session = FakeSession(users=[user])
        monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: FakeReader())
        TestFreeWindowBadge._patch(
            monkeypatch,
            FreeWindow(enabled=True, start="15:00", end="06:00", days=["sat"]),
        )

        response = await rate_limits.get_rate_limits(
            session=session, user=user, ctx=_ctx(10), request=_request(cfg)
        )
        assert response.free.active is True

    @pytest.mark.asyncio
    async def test_fresh_settings_seed_leaves_the_defaults_alone(
        self, frozen_now, monkeypatch
    ):
        """Without a stored row the read seeds one in memory: limits stay
        NULL (inherit) and the free schedule comes back enabled with the
        deployment defaults."""
        cfg = _redis_cfg()
        user = _user(7)
        session = FakeSession(users=[user])
        monkeypatch.setattr(rate_limits, "_reader_factory", lambda cfg: FakeReader())

        response = await rate_limits.get_rate_limits(
            session=session, user=user, ctx=_ctx(10), request=_request(cfg)
        )
        # The seeded schedule is evenings/nights/weekends in UTC; FIXED_NOW
        # (Wed 12:00 UTC) is outside it.
        assert response.free.enabled is True
        assert response.free.active is False
        assert response.free.start == "15:00"
        assert response.free.end == "06:00"
        assert response.free.days == ["sat", "sun"]
        # The user windows still inherit the config-file defaults.
        assert response.user.concurrency_default == 1
