"""Server-side reconstruction of the rate-limit plugin's Redis keys.

The usage endpoint and the usage flusher must read the very counters the
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
(``gpustack.server.gateway_rate_limit_reconciler``), whose group rules decide
which keys exist for a given request path:

* a key with any explicit limit binds a non-fallback combo -- the fallback
  group is suppressed for its requests, so only that combo's windows exist;
  its unset fields inherit the deployment default and its owner's budgets
  are folded in;
* everyone else (no key, or a key without explicit limits) is bound by the
  fallback group: the default plan plus, when set, the owner's budget combo,
  both bucketed on the captured ``gpustack-<user_id>`` segment.

Live reads sit on top of the specs: :class:`RateLimitUsageReader` totals the
plugin's counters per window -- :meth:`~RateLimitUsageReader.read_window` is
the per-window entry point, and :func:`read_windows` the batch helper the
endpoint and the flusher call -- applying the reset semantics each window
kind actually has.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil
from typing import TYPE_CHECKING, List, Optional, Tuple, Union
from zoneinfo import ZoneInfo

import redis.asyncio as redis_asyncio

from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.gateway.rate_limit import WEEK_IN_SECONDS, rate_limit_plugin_name
from gpustack.security import gateway_consumer_identity, is_legacy_token_access_key

if TYPE_CHECKING:
    from gpustack.schemas.api_keys import ApiKey

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
# the plugin declares but the reconciler never publishes.
_CALENDAR_PERIODS = (DAY_PERIOD, MONTH_PERIOD, "each_year")


@dataclass
class RateLimitWindowSpec:
    """One enforced window: its metric identity, quota and Redis key."""

    kind: str  # query | token_rolling | token_calendar
    period: str  # 60s | 604800s | each_day | each_month
    window_seconds: Optional[int]  # rolling only; None for calendar
    limit: int
    redis_key: str


def _has_explicit_limits(key: "ApiKey") -> bool:
    """Whether the key carries a limit of its own, and so binds a key combo."""
    return (
        key.rpm_limit is not None
        or key.tpm_limit is not None
        or key.daily_token_limit is not None
    )


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
        redis_key=f"{RULE_NAME}|{combo}|consumer={identity}|{prefix}:{period}",
    )


def _default_plan_windows(
    rlc: GatewayRateLimitConfig, user_id: int
) -> List[RateLimitWindowSpec]:
    """The deployment defaults exactly as ``default_plan_combination``
    renders them: a window exists only when its limit is configured. The
    match is the user capture, so every window buckets on ``gpustack-<id>``."""
    identity = f"gpustack-{user_id}"
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
    if rlc.default_daily_tokens:
        windows.append(
            _window(
                "token_calendar",
                combo,
                identity,
                DAY_PERIOD,
                None,
                rlc.default_daily_tokens,
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


def user_budget_combo(user_id: int) -> str:
    """The fallback combo carrying one user's shared token budgets:
    ``user-<id>``. Published by the reconciler's ``user_budget_combination``
    and carried verbatim as the second segment of every budget window's
    Redis key — the segment read-side code filters on to tell a user's
    budget windows apart from the deployment defaults."""
    return f"user-{user_id}"


def _user_budget_windows(
    user_id: int, weekly: Optional[int], monthly: Optional[int]
) -> List[RateLimitWindowSpec]:
    """The fallback combo carrying one user's shared token budgets, mirroring
    ``user_budget_combination``. Its capture merges the API-key and UI
    consumer shapes onto the same ``gpustack-<id>`` bucket the default plan
    uses, which is why both fallback combos' keys share the identity."""
    identity = f"gpustack-{user_id}"
    combo = user_budget_combo(user_id)
    windows: List[RateLimitWindowSpec] = []
    if weekly:
        windows.append(
            _window(
                "token_rolling",
                combo,
                identity,
                ROLLING_WEEKLY_PERIOD,
                WEEK_IN_SECONDS,
                weekly,
            )
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


def _key_windows(
    rlc: GatewayRateLimitConfig,
    key: "ApiKey",
    weekly: Optional[int],
    monthly: Optional[int],
) -> List[RateLimitWindowSpec]:
    """The non-fallback combo for one explicitly-limited key, mirroring the
    reconciler's ``key_combination`` field for field: unset key fields
    inherit the deployment default (the fallback is suppressed for these
    requests, so inheriting is what keeps the guard from disappearing), and
    the owner's token budgets are folded in. Monthly folds in only when the
    owner has a budget -- it has no deployment default to fall back on, also
    like the reconciler's key combos."""
    identity = gateway_consumer_identity(key.access_key, key.user_id)
    combo = f"key-{key.id}"
    windows: List[RateLimitWindowSpec] = []
    rpm = key.rpm_limit if key.rpm_limit is not None else rlc.default_rpm
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
    tpm = key.tpm_limit if key.tpm_limit is not None else rlc.default_tpm
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
    if weekly:
        windows.append(
            _window(
                "token_rolling",
                combo,
                identity,
                ROLLING_WEEKLY_PERIOD,
                WEEK_IN_SECONDS,
                weekly,
            )
        )
    daily = (
        key.daily_token_limit
        if key.daily_token_limit is not None
        else rlc.default_daily_tokens
    )
    if daily:
        windows.append(
            _window(
                "token_calendar",
                combo,
                identity,
                DAY_PERIOD,
                None,
                daily,
            )
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


def expected_windows(
    rlc: GatewayRateLimitConfig,
    user_id: int,
    key: Optional["ApiKey"],
    weekly: Optional[int],
    monthly: Optional[int],
) -> List[RateLimitWindowSpec]:
    """Every Redis window the plugin enforces for one user's request path.

    ``rlc`` is the ``gateway_rate_limit`` config (not the top-level
    ``Config``), ``key`` the API key the request authenticates with -- None
    for UI traffic -- and ``weekly`` / ``monthly`` the target user's token
    budgets. A key with any explicit limit yields only its key combo's
    windows; otherwise the fallback group governs, giving the default plan's
    windows followed by the user budget combo's.

    Deployment-token rows (legacy access keys) count as keyless: the
    reconciler never publishes a ``key-<id>`` combo for them, so their
    requests are governed by the fallback group regardless of what limits
    the row carries.

    Calendar windows carry ``window_seconds=None``: their length is not
    fixed, it follows the boundary in ``rlc.timezone`` (see
    :func:`calendar_window_seconds`).
    """
    if (
        key is not None
        and not is_legacy_token_access_key(key.access_key)
        and _has_explicit_limits(key)
    ):
        return _key_windows(rlc, key, weekly, monthly)
    return _default_plan_windows(rlc, user_id) + _user_budget_windows(
        user_id, weekly, monthly
    )


def source_for(
    key: Optional["ApiKey"], weekly: Optional[int], monthly: Optional[int]
) -> str:
    """Which published combinations govern the user's requests: the key's
    own combo (``key``), the fallback group with a user budget
    (``user+default``), or the default plan alone (``default``).

    Legacy deployment-token keys are an intentional divergence from
    :func:`expected_windows`: the reconciler never publishes their
    ``key-<id>`` combo, so the fallback group governs those requests, yet
    this still reports ``key`` -- the field describes the key row's own
    explicit limits, which is what the endpoint displays.
    """
    if key is not None and _has_explicit_limits(key):
        return "key"
    if weekly or monthly:
        return "user+default"
    return "default"


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
    if period not in _CALENDAR_PERIODS:
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
        point endpoint/flusher code should call.

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
        state = await self.window_state(
            spec.redis_key, window_seconds=window_seconds, now=int(now.timestamp())
        )
        if spec.window_seconds is None:
            # Member aging is meaningless for a calendar quota; the count
            # drops at the period boundary.
            state.reset_seconds = calendar_window_seconds(
                spec.period, now, timezone_name
            )
        return state


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

    This is the batch entry point the usage endpoint and the flusher share;
    :meth:`RateLimitUsageReader.read_window` underneath applies the reset
    semantics each window kind actually has.

    Returns:
        (states, error): one state per spec, aligned with ``specs``, or all
        ``None`` with ``error`` set to the first failure's class name.
    """
    results = await asyncio.gather(
        *(
            reader.read_window(spec, now=now, timezone_name=timezone_name)
            for spec in specs
        ),
        return_exceptions=True,
    )
    first_error = next((r for r in results if isinstance(r, BaseException)), None)
    if first_error is None:
        return list(results), None
    usage_error = type(first_error).__name__
    logger.warning("rate-limit window reads failed: %s", usage_error)
    return [None] * len(specs), usage_error


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
