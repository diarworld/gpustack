"""Live rate-limit windows for the Usage page (``GET /usage/rate-limits``).

Pairs the limits the ``gateway_rate_limit`` config, the target user's token
budgets and their API keys publish (via
:func:`gpustack.gateway.rate_limit_usage.expected_windows`) with the live
counters the gateway's ``gpustack-rate-limit`` plugin keeps in Redis, read
through :func:`gpustack.gateway.rate_limit_usage.read_windows`.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import APIRouter, Request
from sqlmodel import select

from gpustack.api.exceptions import NotFoundException
from gpustack.api.tenant import TenantContext, is_platform_wide
from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.gateway.rate_limit_usage import (
    DEFAULT_PLAN_COMBO,
    DAY_PERIOD,
    MONTH_PERIOD,
    ROLLING_WEEKLY_PERIOD,
    RateLimitWindowState,
    RateLimitWindowSpec,
    expected_windows,
    read_windows,
    reader_from_config,
    source_for,
    user_budget_combo,
)
from gpustack.schemas.api_keys import ApiKey
from gpustack.schemas.rate_limits import (
    ApiKeyRateLimits,
    RateLimitWindowUsage,
    RateLimitsResponse,
    UserRateLimits,
)
from gpustack.schemas.users import User
from gpustack.security import is_legacy_token_access_key
from gpustack.server.deps import CurrentUserDep, SessionDep, TenantContextDep

router = APIRouter()

# Module-level indirection so tests can substitute the reader construction;
# production callers get the config-wired factory.
_reader_factory = reader_from_config


def _mode(rlc: Optional[GatewayRateLimitConfig]) -> str:
    """Which counter backend backs the windows: redis, local or disabled."""
    if rlc is None or not rlc.enabled:
        return "disabled"
    return "redis" if rlc.redis is not None else "local"


async def _resolve_target(
    session, user: User, ctx: TenantContext, user_id: Optional[int]
) -> User:
    """The user whose limits to show.

    Self-scoped by construction: a foreign ``user_id`` is honored only for
    a platform-wide admin — admin WITHOUT a pinned Org, the same
    ``is_platform_wide`` gate the usage routes apply to their cross-tenant
    views. An Org-pinned admin is scoped to their Org like anyone else, so
    their foreign ``user_id`` quietly degrades to the caller's own data
    (deny-by-default: the alternative would leak another Org's budgets and
    API keys).
    """
    if user_id in (None, user.id) or not is_platform_wide(user, ctx):
        return user
    target = await User.one_by_id(session, user_id)
    if target is None:
        raise NotFoundException(message="User not found")
    return target


def _window_usage(
    spec: RateLimitWindowSpec, state: Optional[RateLimitWindowState]
) -> RateLimitWindowUsage:
    """Map one spec and its read onto the wire model.

    A ``None`` state (local mode, or a discarded read) omits ``used`` /
    ``reset_seconds``; the response is served with
    ``response_model_exclude_none=True``, so they drop off the wire and the
    UI renders "—".
    """
    return RateLimitWindowUsage(
        kind=spec.kind,
        period=spec.period,
        window_seconds=spec.window_seconds,
        limit=spec.limit,
        used=state.used if state is not None else None,
        reset_seconds=state.reset_seconds if state is not None else None,
    )


@router.get(
    "/rate-limits",
    response_model=RateLimitsResponse,
    response_model_exclude_none=True,
)
async def get_rate_limits(
    session: SessionDep,
    user: CurrentUserDep,
    ctx: TenantContextDep,
    request: Request,
    user_id: Optional[int] = None,
):
    """The target user's rate-limit picture with live window counters.

    Scoping: self by default; a foreign ``user_id`` is honored only for a
    platform-wide admin (``is_platform_wide`` — admin without a pinned
    Org, the usage routes' cross-tenant gate); anything else — non-admins
    and Org-pinned admins alike — silently degrades to the caller's own
    data.

    Args:
        session: async DB session.
        user: the authenticated caller.
        ctx: the request's tenant context; gates the admin expansion.
        request: the FastAPI request; the server config lives on app.state.
        user_id: whose limits to show; defaults to the caller.

    Returns:
        RateLimitsResponse: limits with live counters in redis mode, windows
        without counters in local mode, and empty structures when rate
        limiting is disabled.
    """
    cfg: Config = request.app.state.server_config
    rlc = cfg.gateway_rate_limit
    mode = _mode(rlc)
    # One aware instant shared by every read in this request: the counters
    # and their resets must describe the same moment.
    now = datetime.now(timezone.utc)

    if mode == "disabled":
        # The UI hides the section entirely: no Redis dial, no budget read,
        # not even the key listing — nothing here is meaningful when the
        # plugin enforces nothing.
        return RateLimitsResponse(
            mode=mode, generated_at=now, user=UserRateLimits(), api_keys=[]
        )

    target = await _resolve_target(session, user, ctx, user_id)
    weekly = target.weekly_token_limit
    monthly = target.monthly_token_limit

    # With key=None the spec builder returns the deployment defaults first
    # and this user's budget combo second. Both combos bucket their counters
    # on the user (the plugin captures ``gpustack-<id>`` out of every
    # consumer shape), so every counter here is this user's own traffic. The
    # user block composes the effective per-period view: a budget window
    # overrides the default plan on the period it covers (both counters tick,
    # the budget is always the tighter cap), and the default plan fills the
    # periods the user has no budget for. Per-minute caps stay out — the
    # per-key table renders those.
    all_user_windows = expected_windows(
        rlc, target.id, key=None, weekly=weekly, monthly=monthly
    )
    budget_combo = user_budget_combo(target.id)
    default_by_period: Dict[str, RateLimitWindowSpec] = {}
    budget_by_period: Dict[str, RateLimitWindowSpec] = {}
    for spec in all_user_windows:
        combo = spec.redis_key.split("|")[1]
        if combo == budget_combo:
            budget_by_period[spec.period] = spec
        elif combo == DEFAULT_PLAN_COMBO:
            default_by_period[spec.period] = spec
    user_specs: List[RateLimitWindowSpec] = []
    if DAY_PERIOD in default_by_period:
        user_specs.append(default_by_period[DAY_PERIOD])
    if ROLLING_WEEKLY_PERIOD in budget_by_period:
        user_specs.append(budget_by_period[ROLLING_WEEKLY_PERIOD])
    if MONTH_PERIOD in budget_by_period:
        user_specs.append(budget_by_period[MONTH_PERIOD])
    elif MONTH_PERIOD in default_by_period:
        user_specs.append(default_by_period[MONTH_PERIOD])

    # Same treatment as the reconciler's key pass: every live key of the
    # target, skipping deployment-token rows. Soft-deleted keys no longer
    # authenticate, so they enforce nothing and stay out of the per-key
    # view. Legacy rows are skipped because the reconciler never publishes
    # a ``key-<id>`` combo for them — the fallback group governs their
    # requests — so they carry no per-key windows to show.
    keys = (
        await session.exec(
            select(ApiKey)
            .where(ApiKey.user_id == target.id, ApiKey.deleted_at.is_(None))
            .order_by(ApiKey.id)
        )
    ).all()
    live_keys = [key for key in keys if not is_legacy_token_access_key(key.access_key)]
    key_specs = {
        key.id: expected_windows(
            rlc, target.id, key=key, weekly=weekly, monthly=monthly
        )
        for key in live_keys
    }

    all_specs = list(user_specs)
    for specs in key_specs.values():
        all_specs.extend(specs)

    states: List[Optional[RateLimitWindowState]]
    usage_error = None
    if mode == "redis":
        reader = getattr(request.app.state, "rate_limit_usage_reader", None)
        if reader is None:
            reader = _reader_factory(cfg)
            # "redis" mode implies a reader by construction; cache it for the
            # process (see reader_from_config: one pool, reused connections).
            request.app.state.rate_limit_usage_reader = reader
        states, usage_error = await read_windows(
            reader, all_specs, now=now, timezone_name=rlc.timezone
        )
    else:
        # Local mode keeps no counters: windows render with limits only.
        states = [None] * len(all_specs)

    # Keyed by id(): the specs are plain dataclasses, so two windows of the
    # same shape compare equal and a value-keyed dict would collapse
    # distinct windows onto one read; identity keys cannot collide.
    state_by_spec = {id(spec): state for spec, state in zip(all_specs, states)}
    return RateLimitsResponse(
        mode=mode,
        generated_at=now,
        usage_error=usage_error,
        user=UserRateLimits(
            weekly_limit=weekly,
            monthly_limit=monthly,
            windows=[
                _window_usage(spec, state_by_spec[id(spec)]) for spec in user_specs
            ],
        ),
        api_keys=[
            ApiKeyRateLimits(
                id=key.id,
                name=key.name,
                rpm_limit=key.rpm_limit,
                tpm_limit=key.tpm_limit,
                daily_token_limit=key.daily_token_limit,
                source=source_for(key, weekly, monthly),
                windows=[
                    _window_usage(spec, state_by_spec[id(spec)])
                    for spec in key_specs[key.id]
                ],
            )
            for key in live_keys
        ],
    )
