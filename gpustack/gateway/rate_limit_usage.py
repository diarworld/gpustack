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

Combo semantics mirror the reconciler (``gpustack.server.
gateway_rate_limit_reconciler``), whose group rules decide which keys exist
for a given request path:

* a key with any explicit limit binds a non-fallback combo -- the fallback
  group is suppressed for its requests, so only that combo's windows exist;
  its unset fields inherit the deployment default and its owner's budgets
  are folded in;
* everyone else (no key, or a key without explicit limits) is bound by the
  fallback group: the default plan plus, when set, the owner's budget combo,
  both bucketed on the captured ``gpustack-<user_id>`` segment.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, List, Optional
from zoneinfo import ZoneInfo

from gpustack.config.config import GatewayRateLimitConfig
from gpustack.gateway.rate_limit import WEEK_IN_SECONDS, rate_limit_plugin_name
from gpustack.security import gateway_consumer_identity

if TYPE_CHECKING:
    from gpustack.schemas.api_keys import ApiKey

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


def _user_budget_windows(
    user_id: int, weekly: Optional[int], monthly: Optional[int]
) -> List[RateLimitWindowSpec]:
    """The fallback combo carrying one user's shared token budgets, mirroring
    ``user_budget_combination``. Its capture merges the API-key and UI
    consumer shapes onto the same ``gpustack-<id>`` bucket the default plan
    uses, which is why both fallback combos' keys share the identity."""
    identity = f"gpustack-{user_id}"
    combo = f"user-{user_id}"
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

    Calendar windows carry ``window_seconds=None``: their length is not
    fixed, it follows the boundary in ``rlc.timezone`` (see
    :func:`calendar_window_seconds`).
    """
    if key is not None and _has_explicit_limits(key):
        return _key_windows(rlc, key, weekly, monthly)
    return _default_plan_windows(rlc, user_id) + _user_budget_windows(
        user_id, weekly, monthly
    )


def source_for(
    key: Optional["ApiKey"], weekly: Optional[int], monthly: Optional[int]
) -> str:
    """Which published combinations govern the user's requests: the key's
    own combo (``key``), the fallback group with a user budget
    (``user+default``), or the default plan alone (``default``)."""
    if key is not None and _has_explicit_limits(key):
        return "key"
    if weekly or monthly:
        return "user+default"
    return "default"


def calendar_window_seconds(period: str, now: datetime, timezone_name: str) -> int:
    """Seconds from ``now`` to the next boundary of a calendar period, in
    the deployment timezone.

    The plugin enforces each calendar quota as a sliding window from the
    period's start; the endpoint reports when the quota resets instead, so
    this returns the complement -- the distance to the period's end. All
    arithmetic runs on aware datetimes in the target zone, matching the
    plugin's own ``periodEnd`` computation.
    """
    if period not in _CALENDAR_PERIODS:
        raise ValueError(f"unknown calendar period: {period!r}")
    local = now.astimezone(ZoneInfo(timezone_name))
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == DAY_PERIOD:
        boundary = midnight + timedelta(days=1)
    elif period == MONTH_PERIOD:
        next_month = local.month % 12 + 1
        next_year = local.year + 1 if local.month == 12 else local.year
        boundary = midnight.replace(year=next_year, month=next_month, day=1)
    else:
        boundary = midnight.replace(year=local.year + 1, month=1, day=1)
    return int((boundary - local).total_seconds())
