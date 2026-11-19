"""Live rate-limit windows for the Usage page (``GET /usage/rate-limits``).

Pairs the limits the ``gateway_rate_limit`` config, the target user's token
budgets and their API keys publish (via
:func:`gpustack.gateway.rate_limit_usage.expected_windows`) with the live
counters the gateway's ``gpustack-rate-limit`` plugin keeps in Redis, read
through :func:`gpustack.gateway.rate_limit_usage.read_windows`.
"""

from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Request
from sqlmodel import select

from gpustack.api.exceptions import NotFoundException
from gpustack.api.tenant import TenantContext, is_platform_wide
from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.gateway.rate_limit_usage import (
    RateLimitWindowState,
    RateLimitWindowSpec,
    key_windows,
    read_windows,
    reader_from_config,
    source_for,
    user_limit_windows,
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

    # The user limit combo is non-fallback and governs every request of the
    # user, so these windows are the effective user-level picture: requests
    # and tokens per minute (the user's setting or the deployment default),
    # the weekly budget (setting only) and the monthly quota (setting or
    # default). All bucket on ``gpustack-<id>`` -- the user's own traffic
    # across all their keys.
    user_specs = user_limit_windows(rlc, target.id, target)

    # Every live key of the target adds its own day quota on top, skipping
    # deployment-token rows. Soft-deleted keys no longer authenticate, so
    # they enforce nothing and stay out of the per-key view. Legacy rows are
    # skipped because the reconciler never publishes a ``key-<id>`` combo
    # for them -- only the user combo governs their requests -- so they
    # carry no per-key windows to show.
    keys = (
        await session.exec(
            select(ApiKey)
            .where(ApiKey.user_id == target.id, ApiKey.deleted_at.is_(None))
            .order_by(ApiKey.id)
        )
    ).all()
    live_keys = [key for key in keys if not is_legacy_token_access_key(key.access_key)]
    key_specs = {key.id: key_windows(rlc, key) for key in live_keys}

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
            rpm_limit=target.rpm_limit,
            tpm_limit=target.tpm_limit,
            weekly_limit=target.weekly_token_limit,
            monthly_limit=target.monthly_token_limit,
            windows=[
                _window_usage(spec, state_by_spec[id(spec)]) for spec in user_specs
            ],
        ),
        api_keys=[
            ApiKeyRateLimits(
                id=key.id,
                name=key.name,
                daily_token_limit=key.daily_token_limit,
                source=source_for(key),
                windows=[
                    _window_usage(spec, state_by_spec[id(spec)])
                    for spec in key_specs[key.id]
                ],
            )
            for key in live_keys
        ],
    )
