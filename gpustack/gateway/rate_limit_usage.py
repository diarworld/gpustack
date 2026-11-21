"""Server-side reconstruction of the rate-limit plugin's Redis keys.

The usage endpoint must read the very counters the
gateway's ``gpustack-rate-limit`` plugin writes. The plugin composes those
keys from the published combinations, so this module re-derives them from
the same inputs -- the ``gateway_rate_limit`` config, an API key, and its
owner's token budgets. The byte layout is the plugin's documented one::

    <rule_name>|<combo.Name>|<dim fragments joined by "|">|<kind>:<period>

where ``kind`` is ``q`` for rolling query windows, ``t`` for token windows
(rolling periods render as ``<seconds>s``, calendar ones as ``each_day`` /
``each_month``), and the consumer dimension fragment is ``consumer=<value>``.
A key that is off by one byte reads an empty counter, so the golden strings
in ``tests/gateway/test_rate_limit_usage.py`` pin the layout.

Combo semantics mirror the reconciler
(``gpustack.server.gateway_rate_limit_reconciler``): every limit is
user-level -- the user limit combo is non-fallback and binds every request
of a user with keys, so only the default-plan fallback windows exist for
requests outside it, both bucketed on the captured ``gpustack-<user_id>``
segment.

Live reads sit on top of the specs: :class:`RateLimitUsageReader` totals the
plugin's counters per window -- :meth:`~RateLimitUsageReader.read_window` is
the per-window entry point, :meth:`~RateLimitUsageReader.window_states` the
pipelined batch, and :func:`read_windows` the fail-open wrapper the endpoint calls -- applying the reset semantics each window kind
actually has.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil
from typing import TYPE_CHECKING, List, Optional, Tuple, Union
from zoneinfo import ZoneInfo

import redis.asyncio as redis_asyncio

from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.gateway.rate_limit import WEEK_IN_SECONDS, rate_limit_plugin_name

if TYPE_CHECKING:
    from gpustack.schemas.api_keys import ApiKey
    from gpustack.schemas.principals import Principal

logger = logging.getLogger(__name__)

RULE_NAME = rate_limit_plugin_name

# The combo names the reconciler publishes; the Redis key carries them
# verbatim as its second segment.
DEFAULT_PLAN_COMBO = "default-plan"

MINUTE_IN_SECONDS = 60

# The rolling periods the reconciler ever declares: per-minute limits and the
# sliding-weekly token budget. The plugin renders a rolling period as its
# window size in seconds followed by "s".
ROLLING_QUERY_PERIOD = "60s"
ROLLING_TOKEN_PERIOD = "60s"
ROLLING_WEEKLY_PERIOD = f"{WEEK_IN_SECONDS}s"

# Calendar periods (the plugin's PeriodKind) the reconciler ever declares.
DAY_PERIOD = "each_day"
MONTH_PERIOD = "each_month"

# The plugin's full calendar vocabulary: the pair above plus each_year, which
# the plugin declares but the reconciler never publishes. Public: read-side
# consumers (the usage rebuilder) parse every suffix the plugin can write.
CALENDAR_PERIODS = (DAY_PERIOD, MONTH_PERIOD, "each_year")

# The key layout's dim-fragment and identity vocabulary: the consumer
# dimension renders as ``consumer=<identity>``, and the user-bucketed
# identity the fallback combos capture is ``gpustack-<user_id>``. The byte
# layout of the Redis keys is the plugin's documented one (see the module
# docstring), so every builder and parser composes from these constants.
CONSUMER_DIM_PREFIX = "consumer="
USER_IDENTITY_PREFIX = "gpustack-"


@dataclass
class RateLimitWindowSpec:
    """One enforced window: its metric identity, quota and Redis key."""

    kind: str  # query | token_rolling | token_calendar
    period: str  # 60s | 604800s | each_day | each_month
    window_seconds: Optional[int]  # rolling only; None for calendar
    limit: int
    redis_key: str


def _window(
    kind: str,
    combo: str,
    identity: str,
    period: str,
    window_seconds: Optional[int],
    limit: int,
) -> RateLimitWindowSpec:
    """One window spec with its Redis key. Query limits prefix their period
    with ``q``, every token window with ``t`` -- the plugin's two kinds."""
    prefix = "q" if kind == "query" else "t"
    return RateLimitWindowSpec(
        kind=kind,
        period=period,
        window_seconds=window_seconds,
        limit=limit,
        redis_key=(
            f"{RULE_NAME}|{combo}|{CONSUMER_DIM_PREFIX}{identity}|{prefix}:{period}"
        ),
    )


def default_plan_windows(
    rlc: GatewayRateLimitConfig, user_id: int
) -> List[RateLimitWindowSpec]:
    """The deployment defaults exactly as ``default_plan_combination``
    renders them: a window exists only when its limit is configured. The
    match is the user capture, so every window buckets on ``gpustack-<id>``.

    The fallback plan no longer governs any live request -- the user limit
    combos are non-fallback and always match first -- but the rebuilder still
    restores the Redis keys it left behind until their TTLs expire."""
    identity = f"{USER_IDENTITY_PREFIX}{user_id}"
    combo = DEFAULT_PLAN_COMBO
    windows: List[RateLimitWindowSpec] = []
    if rlc.default_rpm:
        windows.append(
            _window(
                "query",
                combo,
                identity,
                ROLLING_QUERY_PERIOD,
                MINUTE_IN_SECONDS,
                rlc.default_rpm,
            )
        )
    if rlc.default_tpm:
        windows.append(
            _window(
                "token_rolling",
                combo,
                identity,
                ROLLING_TOKEN_PERIOD,
                MINUTE_IN_SECONDS,
                rlc.default_tpm,
            )
        )
    if rlc.default_monthly_tokens:
        windows.append(
            _window(
                "token_calendar",
                combo,
                identity,
                MONTH_PERIOD,
                None,
                rlc.default_monthly_tokens,
            )
        )
    return windows


class _UserLimits:
    """The four user-level limit fields off a Principal row, tolerating a
    missing row (the rebuilder's default-plan path) as all-None."""

    def __init__(self, user: Optional["Principal"]):
        self.rpm = getattr(user, "rpm_limit", None)
        self.tpm = getattr(user, "tpm_limit", None)
        self.weekly = getattr(user, "weekly_token_limit", None)
        self.monthly = getattr(user, "monthly_token_limit", None)


def user_limit_combo(user_id: int) -> str:
    """The combo carrying one user's shared limits: ``user-<id>``. Published
    by the reconciler's ``user_limits_combination`` and carried verbatim as
    the second segment of every user-level window's Redis key — the segment
    read-side code filters on."""
    return f"user-{user_id}"


def user_limit_windows(
    rlc: GatewayRateLimitConfig, user_id: int, user: Optional["Principal"]
) -> List[RateLimitWindowSpec]:
    """The non-fallback combo carrying one user's shared limits, mirroring
    the reconciler's ``user_limits_combination`` field for field: requests
    and tokens per minute, the weekly rolling budget and the monthly
    calendar quota. Unset user fields inherit the deployment defaults --
    the key combos these requests also match suppress the fallback default
    plan, so inheriting is what keeps the deployment plan in force. Its
    capture merges the API-key and UI consumer shapes onto the same
    ``gpustack-<id>`` bucket."""
    identity = f"{USER_IDENTITY_PREFIX}{user_id}"
    combo = user_limit_combo(user_id)
    limits = _UserLimits(user)

    windows: List[RateLimitWindowSpec] = []
    rpm = limits.rpm if limits.rpm is not None else rlc.default_rpm
    if rpm:
        windows.append(
            _window(
                "query",
                combo,
                identity,
                ROLLING_QUERY_PERIOD,
                MINUTE_IN_SECONDS,
                rpm,
            )
        )
    tpm = limits.tpm if limits.tpm is not None else rlc.default_tpm
    if tpm:
        windows.append(
            _window(
                "token_rolling",
                combo,
                identity,
                ROLLING_TOKEN_PERIOD,
                MINUTE_IN_SECONDS,
                tpm,
            )
        )
    if limits.weekly:
        windows.append(
            _window(
                "token_rolling",
                combo,
                identity,
                ROLLING_WEEKLY_PERIOD,
                WEEK_IN_SECONDS,
                limits.weekly,
            )
        )
    monthly = (
        limits.monthly if limits.monthly is not None else rlc.default_monthly_tokens
    )
    if monthly:
        windows.append(
            _window(
                "token_calendar",
                combo,
                identity,
                MONTH_PERIOD,
                None,
                monthly,
            )
        )
    return windows


@dataclass
class ModelConcurrencySpec:
    """One per-model in-flight cap: which slot counter it tracks."""

    model: str
    # 0 = explicit unlimited for this model.
    limit: int
    redis_key: str


def model_concurrency_specs(
    rlc: GatewayRateLimitConfig,
    user_id: int,
    user: Optional["Principal"],
    models: List[str],
) -> List[ModelConcurrencySpec]:
    """The per-model in-flight caps enforced for one user, over the ready
    route names ``models``, mirroring the reconciler's combo layout: the
    user limit combo carries the ``concurrency_limits`` block, and the
    plugin keys one ZSET per (combo, model) with the ``c:<model>`` suffix
    and the same consumer dimension fragment as every other user-level
    window.

    The block restates the deployment ``default_concurrency`` as its
    ``default``, so every model resolves to a cap -- the user's explicit
    override when named, the deployment default otherwise -- and the
    plugin keeps a slot counter for every model with a non-zero resolved
    quota, whether or not the user's row names an override. A model with
    no cap left to report (no override on a deployment with the cap off)
    is dropped rather than itemized.

    Admins are exempt from the cap: their combo omits the block, so no
    counter exists for their requests and an admin row itemizes nothing.
    """
    if getattr(user, "is_admin", False):
        return []
    overrides = getattr(user, "model_concurrency_limits", None) or {}
    identity = f"{USER_IDENTITY_PREFIX}{user_id}"
    combo = user_limit_combo(user_id)
    specs = []
    for model in models:
        if model in overrides:
            limit = overrides[model]
        elif rlc.default_concurrency:
            limit = rlc.default_concurrency
        else:
            continue
        specs.append(
            ModelConcurrencySpec(
                model=model,
                limit=limit,
                redis_key=(
                    f"{RULE_NAME}|{combo}|{CONSUMER_DIM_PREFIX}{identity}|c:{model}"
                ),
            )
        )
    return specs


def expected_windows(
    rlc: GatewayRateLimitConfig,
    user_id: int,
    key: Optional["ApiKey"],
    user: Optional["Principal"],
) -> List[RateLimitWindowSpec]:
    """Every Redis window the plugin enforces for one user's request path.

    ``rlc`` is the ``gateway_rate_limit`` config (not the top-level
    ``Config``), ``key`` the API key the request authenticates with -- None
    for UI traffic -- and ``user`` the target user's row carrying the four
    user-level limit fields (``rpm_limit``, ``tpm_limit``,
    ``weekly_token_limit``, ``monthly_token_limit``). The user limit combo
    governs every request; ``key`` is accepted (and ignored) so callers can
    pass the authenticated principal without branching.

    Deployment-token rows (legacy access keys) count as keyless: only the
    user combo governs their requests.

    Calendar windows carry ``window_seconds=None``: their length is not
    fixed, it follows the boundary in ``rlc.timezone`` (see
    :func:`calendar_window_seconds`).
    """
    return user_limit_windows(rlc, user_id, user)


def _calendar_period_bounds(
    period: str, now: datetime, zone: ZoneInfo
) -> Tuple[datetime, datetime]:
    """Start and end (exclusive) of the calendar period holding ``now``,
    rendered in ``zone``.

    ``now`` must be timezone-aware: converting a naive stamp would silently
    read it as the server's local zone and shift every boundary.
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if period not in CALENDAR_PERIODS:
        raise ValueError(f"unknown calendar period: {period!r}")
    local = now.astimezone(zone)
    start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == DAY_PERIOD:
        end = start + timedelta(days=1)
    elif period == MONTH_PERIOD:
        start = start.replace(day=1)
        next_month = local.month % 12 + 1
        next_year = local.year + 1 if local.month == 12 else local.year
        end = start.replace(year=next_year, month=next_month, day=1)
    else:
        start = start.replace(month=1, day=1)
        end = start.replace(year=local.year + 1, month=1, day=1)
    return start, end


def calendar_window_seconds(period: str, now: datetime, timezone_name: str) -> int:
    """Seconds from ``now`` to the next boundary of a calendar period, in
    the deployment timezone.

    The plugin enforces each calendar quota as a sliding window from the
    period's start; the endpoint reports when the quota resets instead, so
    this returns the complement -- the distance to the period's end. All
    arithmetic runs on aware datetimes in the target zone, matching the
    plugin's own ``periodEnd`` computation.
    """
    zone = ZoneInfo(timezone_name)
    _, end = _calendar_period_bounds(period, now, zone)
    return int((end - now.astimezone(zone)).total_seconds())


def calendar_elapsed_seconds(period: str, now: datetime, timezone_name: str) -> int:
    """Seconds elapsed since the calendar period holding ``now`` began, in
    the deployment timezone.

    The complement of :func:`calendar_window_seconds` -- the two always sum
    to the period's full length. Calendar callers pass this as the reader's
    ``window_seconds``, bounding the read to the live portion of the period.
    """
    zone = ZoneInfo(timezone_name)
    start, _ = _calendar_period_bounds(period, now, zone)
    return max(0, int((now.astimezone(zone) - start).total_seconds()))


@dataclass
class RateLimitWindowState:
    """Live total of one window plus when its count will drop."""

    used: int  # summed member counts inside the window
    reset_seconds: int  # 0 when the window is empty


def parse_member_count(member: Union[str, bytes]) -> int:
    """Count carried by one ZSET member: ``'<uuid>|<count>'``; a bare legacy
    id counts 1.

    Mirrors the plugin's own parse (``string.match(m, '|(%d+)$')``): the
    trailing ``|<count>`` is the weight, and anything without one -- bare
    members, malformed tails, non-ASCII digit look-alikes -- counts as 1.
    """
    if isinstance(member, bytes):
        member = member.decode("utf-8", errors="replace")
    _, separator, tail = member.rpartition("|")
    if separator and tail.isascii() and tail.isdigit():
        return int(tail)
    return 1


class RateLimitUsageReader:
    """Reads live window totals from the plugin's Redis counters.

    Dependency-injected client keeps tests offline; :func:`reader_from_config`
    wires the real client. The caller owns clock and window math -- every
    method takes ``now`` explicitly.
    """

    def __init__(self, client: redis_asyncio.Redis):
        self._client = client

    @property
    def client(self) -> redis_asyncio.Redis:
        """The underlying client, for SCAN-style sweeps by long-lived
        consumers (the usage rebuilder); tests reach the dial plan through it."""
        return self._client

    async def window_state(
        self, redis_key: str, *, window_seconds: int, now: int
    ) -> RateLimitWindowState:
        """Total recorded in one window and when that total will drop.

        Low-level primitive over one Redis key: the reset it reports is
        always member aging, which is only true for rolling windows. Prefer
        :meth:`read_window`, which applies the reset semantics each window
        kind actually has.

        Mirrors the plugin's prune-then-sum boundary: it discards members
        scored at or before ``now - window_seconds`` before summing, so the
        read's lower bound is exclusive while ``now`` stays inclusive.

        For calendar windows the caller overrides ``reset_seconds`` -- a
        calendar quota drops at the period boundary, not when an in-window
        member ages out -- and passes ``window_seconds`` as the elapsed
        period length (see :func:`calendar_elapsed_seconds`), bounding the
        read to the live portion of the period.
        """
        members = await self._client.zrangebyscore(
            redis_key, f"({now - window_seconds}", now, withscores=True
        )
        if not members:
            return RateLimitWindowState(used=0, reset_seconds=0)
        used = sum(parse_member_count(member) for member, _ in members)
        oldest = min(score for _, score in members)
        reset = max(0, ceil(oldest + window_seconds - now))
        return RateLimitWindowState(used=used, reset_seconds=reset)

    async def read_window(
        self, spec: RateLimitWindowSpec, *, now: datetime, timezone_name: str
    ) -> RateLimitWindowState:
        """Window state with the reset semantics each kind actually has:
        rolling = member aging, calendar = period boundary. The only entry
        point for a single window; batches go through :meth:`window_states`.

        The one ``now`` drives both the read instant and the calendar
        boundary math, so the two cannot drift apart; ``timezone_name`` is
        the deployment timezone anchoring calendar periods.

        ``now`` must be timezone-aware.
        """
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        # Calendar specs carry no fixed length: bound the read to the live
        # portion of the running period.
        window_seconds = spec.window_seconds or calendar_elapsed_seconds(
            spec.period, now, timezone_name
        )
        members = await self._client.zrangebyscore(
            spec.redis_key,
            f"({int(now.timestamp()) - window_seconds}",
            int(now.timestamp()),
            withscores=True,
        )
        return self._state_from_members(
            spec,
            members,
            window_seconds=window_seconds,
            now=now,
            timezone_name=timezone_name,
        )

    async def window_states(
        self,
        specs: List[RateLimitWindowSpec],
        *,
        now: datetime,
        timezone_name: str,
    ) -> List[RateLimitWindowState]:
        """Every window state in one pipeline round trip.

        The endpoint reads a whole user's picture and the rebuilder sweeps
        every user's windows -- hundreds of windows a batch. One concurrent
        ``ZRANGEBYSCORE`` per window blew through the client's connection
        pool (redis-py raises ``MaxConnectionsError`` past its cap) and
        cost a round trip each; a pipeline queues every command on ONE
        connection, so batch size costs no connections at all.

        One ``now`` drives every read and every calendar boundary, so the
        states describe a single instant. ``now`` must be timezone-aware.
        Any command-level failure raises -- :func:`read_windows` turns that
        into the fail-open blank batch.
        """
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        now_ts = int(now.timestamp())
        pipe = self._client.pipeline(transaction=False)
        bounds: List[int] = []
        for spec in specs:
            # Calendar specs carry no fixed length: bound the read to the
            # live portion of the running period.
            window_seconds = spec.window_seconds or calendar_elapsed_seconds(
                spec.period, now, timezone_name
            )
            bounds.append(window_seconds)
            pipe.zrangebyscore(
                spec.redis_key,
                f"({now_ts - window_seconds}",
                now_ts,
                withscores=True,
            )
        results = await pipe.execute(raise_on_error=False)
        first_error = next((r for r in results if isinstance(r, BaseException)), None)
        if first_error is not None:
            raise first_error
        return [
            self._state_from_members(
                spec,
                members,
                window_seconds=window_seconds,
                now=now,
                timezone_name=timezone_name,
            )
            for spec, window_seconds, members in zip(specs, bounds, results)
        ]

    def _state_from_members(
        self,
        spec: RateLimitWindowSpec,
        members: List[Union[str, bytes]],
        *,
        window_seconds: int,
        now: datetime,
        timezone_name: str,
    ) -> RateLimitWindowState:
        """Sum the pruned members and apply the reset semantics each window
        kind actually has: rolling = member aging, calendar = the period's
        end. Shared by the single and batched reads so the two cannot drift
        apart."""
        if not members:
            state = RateLimitWindowState(used=0, reset_seconds=0)
        else:
            used = sum(parse_member_count(member) for member, _ in members)
            oldest = min(score for _, score in members)
            state = RateLimitWindowState(
                used=used,
                reset_seconds=max(
                    0, ceil(oldest + window_seconds - int(now.timestamp()))
                ),
            )
        if spec.window_seconds is None:
            # Member aging is meaningless for a calendar quota; the count
            # drops at the period boundary.
            state.reset_seconds = calendar_window_seconds(
                spec.period, now, timezone_name
            )
        return state

    async def concurrency_counts(self, keys: List[str]) -> List[int]:
        """Current ZSET size per key, one pipeline round trip.

        The concurrency counter is the plain cardinality of the slot ZSET:
        one member per running request, so ``ZCARD`` reads "running right
        now" without any window math -- stale slots the plugin has not
        pruned yet inflate the number, which is exactly what the gateway
        enforces against too. Commands run in the same pipelined shape as
        :meth:`window_states` (one connection, no per-key round trip); any
        command-level failure raises so the caller can fail open.
        """
        pipe = self._client.pipeline(transaction=False)
        for key in keys:
            pipe.zcard(key)
        results = await pipe.execute(raise_on_error=False)
        first_error = next((r for r in results if isinstance(r, BaseException)), None)
        if first_error is not None:
            raise first_error
        return [int(count) for count in results]


async def read_windows(
    reader: RateLimitUsageReader,
    specs: List[RateLimitWindowSpec],
    *,
    now: datetime,
    timezone_name: str,
) -> Tuple[Optional[List[RateLimitWindowState]], Optional[str]]:
    """Read every window at once, failing open.

    All reads share one ``now`` so the windows and their resets describe a
    single instant. Any failure (Redis down, a bad ``timezone`` raising
    ``ZoneInfoNotFoundError`` in the calendar math) discards the whole
    batch: the consumer degrades to "—" everywhere instead of mixing fresh
    and stale counters. Only the exception's class name surfaces — its
    message can carry hostnames.

    This is the batch entry point the usage endpoint and the usage
    rebuilder share.
    """
    try:
        states = await reader.window_states(specs, now=now, timezone_name=timezone_name)
        return states, None
    except Exception as exc:
        usage_error = type(exc).__name__
        logger.warning("rate-limit window reads failed: %s", usage_error)
        return [None] * len(specs), usage_error


async def read_concurrency(
    reader: RateLimitUsageReader, keys: List[str]
) -> Tuple[Optional[List[int]], Optional[str]]:
    """Read every concurrency counter at once, failing open.

    Same contract as :func:`read_windows`: any failure discards the whole
    batch (the consumer renders "—") and only the exception's class name
    surfaces.
    """
    if not keys:
        return [], None
    try:
        return await reader.concurrency_counts(keys), None
    except Exception as exc:
        usage_error = type(exc).__name__
        logger.warning("rate-limit concurrency reads failed: %s", usage_error)
        return None, usage_error


def reader_from_config(cfg: Config) -> Optional[RateLimitUsageReader]:
    """Reader over the rate-limit Redis service; None when unset (local
    mode).

    Dials the ``gateway_rate_limit.redis`` service -- the SAME instance the
    gateway pods write counters to -- not the server's top-level
    ``redis_url``. ``redis.asyncio.Redis`` connects lazily, so building the
    reader never dials.

    Cache one reader per process (e.g. on FastAPI's ``app.state`` at
    startup): redis-py binds connections lazily, so per-request construction
    churns connection pools, while a module-level singleton breaks under
    pytest-asyncio's per-test event loops.
    """
    rlc = cfg.gateway_rate_limit
    if rlc is None or rlc.redis is None:
        return None
    service = rlc.redis
    timeout = service.timeout / 1000
    client = redis_asyncio.Redis(
        host=service.service_name,
        port=service.service_port,
        username=service.username,
        password=service.password,
        db=service.database,
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
        # Counters stay byte-faithful: the plugin's members are ASCII and
        # parse_member_count tolerates bytes either way.
        decode_responses=False,
    )
    return RateLimitUsageReader(client)
