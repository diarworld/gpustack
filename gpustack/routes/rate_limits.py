"""Live rate-limit windows for the Usage page (``GET /usage/rate-limits``).

Pairs the limits the ``gateway_rate_limit`` config and the target user's
token budgets publish (via
:func:`gpustack.gateway.rate_limit_usage.expected_windows`) with the live
counters the gateway's ``gpustack-rate-limit`` plugin keeps in Redis, read
through :func:`gpustack.gateway.rate_limit_usage.read_windows`.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter, Request
from sqlalchemy import or_
from sqlmodel import select

from gpustack.api.exceptions import (
    BadRequestException,
    InvalidException,
    NotFoundException,
)
from gpustack.api.tenant import TenantContext, is_platform_wide
from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.gateway.rate_limit_usage import (
    RateLimitWindowState,
    RateLimitWindowSpec,
    default_plan_windows,
    model_concurrency_specs,
    read_concurrency,
    read_windows,
    reader_from_config,
    user_limit_windows,
)
from gpustack.schemas.gateway_rate_limit_resets import GatewayRateLimitReset
from gpustack.schemas.model_routes import MyModel
from gpustack.schemas.rate_limits import (
    ModelConcurrencyUsage,
    RateLimitResetCreate,
    RateLimitResetStatus,
    RateLimitWindowUsage,
    RateLimitsResponse,
    UserRateLimits,
)
from gpustack.schemas.users import User
from gpustack.server.deps import CurrentUserDep, SessionDep, TenantContextDep

router = APIRouter()

# Module-level indirection so tests can substitute the reader construction;
# production callers get the config-wired factory.
_reader_factory = reader_from_config

# Resets allowed per floating window, per reset kind. The window floats
# with the presses themselves: two presses in one week make the next press
# available a week after the first one, not on the next calendar boundary.
RESET_QUOTA = 2
RESET_WINDOW_SECONDS = {"weekly": 604800, "monthly": 30 * 86400}
# The long token window periods each reset kind forgives, matched against
# the published windows of the target user.
_RESET_PERIODS = {"weekly": "604800s", "monthly": "each_month"}


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


def _reader(request: Request, cfg: Config):
    """The config-wired Redis reader, cached on the app for the process."""
    reader = getattr(request.app.state, "rate_limit_usage_reader", None)
    if reader is None:
        reader = _reader_factory(cfg)
        # "redis" mode implies a reader by construction; cache it for the
        # process (see reader_from_config: one pool, reused connections).
        request.app.state.rate_limit_usage_reader = reader
    return reader


async def _ready_model_names(session, target: User) -> List[str]:
    """The ready routes the target can reach, by name.

    A non-admin's reachable set is the ``non_admin_user_models`` view: its
    ``user_id`` rows encode the visibility grants, and ``ready_targets > 0``
    mirrors the READY filter the models page uses. These are the model
    names the gateway's routing header carries for this user's traffic --
    the fragments the plugin's concurrency counters key on. Admins are
    exempt from the concurrency cap (their combo omits the block, no
    counter exists), so there is nothing to itemize for them and no query
    is spent.
    """
    if target.is_admin:
        return []
    stmt = select(MyModel.name).where(
        MyModel.deleted_at.is_(None),
        MyModel.ready_targets > 0,
        MyModel.user_id == target.id,
    )
    rows = (await session.exec(stmt)).all()
    return sorted(set(rows))


async def _reset_status(
    session, target_id: int, kind: str, now: datetime
) -> RateLimitResetStatus:
    """Presses still available for one window kind under the floating quota.

    Only self-service presses count against the quota: an admin resetting
    on the user's behalf (``reset_by_user_id`` pointing at someone else)
    is a support action and never consumes the user's own allowance.
    """
    window = timedelta(seconds=RESET_WINDOW_SECONDS[kind])
    counted = (
        await session.exec(
            select(GatewayRateLimitReset)
            .where(
                GatewayRateLimitReset.user_id == target_id,
                GatewayRateLimitReset.kind == kind,
                GatewayRateLimitReset.reset_at > now - window,
                or_(
                    GatewayRateLimitReset.reset_by_user_id.is_(None),
                    GatewayRateLimitReset.reset_by_user_id == target_id,
                ),
            )
            .order_by(GatewayRateLimitReset.reset_at)
        )
    ).all()
    available = RESET_QUOTA - len(counted)
    # The oldest counted press ages out of the floating window first; when
    # it does, its allowance frees up for the next press.
    next_available_at = counted[0].reset_at + window if available <= 0 else None
    return RateLimitResetStatus(
        quota=RESET_QUOTA,
        available=max(available, 0),
        next_available_at=next_available_at,
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
        # The UI hides the section entirely: no Redis dial, no budget read
        # — nothing here is meaningful when the plugin enforces nothing.
        return RateLimitsResponse(mode=mode, generated_at=now, user=UserRateLimits())

    target = await _resolve_target(session, user, ctx, user_id)

    # The user limit combo is non-fallback and governs every request of the
    # user, so these windows are the effective user-level picture: requests
    # and tokens per minute (the user's setting or the deployment default),
    # the weekly budget (setting only) and the monthly quota (setting or
    # default). All bucket on ``gpustack-<id>`` -- the user's own traffic
    # across all their keys.
    user_specs = user_limit_windows(rlc, target.id, target)
    all_specs = list(user_specs)

    states: List[Optional[RateLimitWindowState]]
    usage_error = None
    if mode == "redis":
        reader = _reader(request, cfg)
        states, usage_error = await read_windows(
            reader, all_specs, now=now, timezone_name=rlc.timezone
        )
    else:
        # Local mode keeps no counters: windows render with limits only.
        states = [None] * len(all_specs)

    # Per-model in-flight caps with their live slot counts, over the ready
    # models the target can reach. Each row is personal: ``running`` is the
    # user's own slot count from the plugin's per-user counter, and the
    # limit is their override or the deployment default. Local mode has no
    # server-readable counters, so ``running`` stays unset there; admins
    # are exempt and itemize nothing.
    ready_models = await _ready_model_names(session, target)
    concurrency_specs = model_concurrency_specs(rlc, target.id, target, ready_models)
    concurrency_running: List[Optional[int]] = [None] * len(concurrency_specs)
    concurrency_error = None
    if concurrency_specs and mode == "redis":
        reader = _reader(request, cfg)
        counts, concurrency_error = await read_concurrency(
            reader, [spec.redis_key for spec in concurrency_specs]
        )
        if concurrency_error is not None:
            usage_error = usage_error or concurrency_error
        else:
            concurrency_running = list(counts)

    # Display order the Usage page renders: busiest model first (a row the
    # read could not fill in sorts last), name as the tiebreaker.
    model_usage = [
        ModelConcurrencyUsage(model=spec.model, limit=spec.limit, running=running)
        for spec, running in zip(concurrency_specs, concurrency_running)
    ]
    model_usage.sort(
        key=lambda row: (row.running is None, -(row.running or 0), row.model)
    )

    # Reset-button state only exists where there are counters to clear;
    # in local mode the plugin keeps nothing a reset could forgive.
    resets = None
    if mode == "redis":
        resets = {
            kind: await _reset_status(session, target.id, kind, now)
            for kind in RESET_WINDOW_SECONDS
        }

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
            concurrency_default=rlc.default_concurrency,
            windows=[
                _window_usage(spec, state_by_spec[id(spec)]) for spec in user_specs
            ],
            model_concurrency=model_usage,
            resets=resets,
        ),
    )


@router.post("/rate-limits/reset", response_model=RateLimitResetStatus)
async def reset_rate_limit(
    body: RateLimitResetCreate,
    session: SessionDep,
    user: CurrentUserDep,
    ctx: TenantContextDep,
    request: Request,
):
    """Forgive the target user's weekly or monthly token budget mid-window.

    Deletes the gateway's Redis counters of that window kind (the budget
    restarts from zero on the next request), records the press in
    ``gateway_rate_limit_resets`` and returns the fresh quota state.

    Scoping mirrors ``get_rate_limits``: self by default, a foreign
    ``user_id`` only for a platform-wide admin. An admin bypasses the
    floating quota — their press is a support action, recorded with
    ``reset_by_user_id`` and never counted against the user's own
    allowance.

    Args:
        body: the window kind, the required reason note, and optionally
            the user to reset (admin expansion).
        session: async DB session.
        user: the authenticated caller.
        ctx: the request's tenant context; gates the admin expansion.
        request: the FastAPI request; the server config lives on app.state.

    Returns:
        RateLimitResetStatus: the quota state after the press.
    """
    cfg: Config = request.app.state.server_config
    rlc = cfg.gateway_rate_limit
    if _mode(rlc) != "redis":
        raise InvalidException(
            message="Rate-limit resets require the Redis counter backend"
        )
    if body.kind not in _RESET_PERIODS:
        raise InvalidException(message=f"kind must be one of {sorted(_RESET_PERIODS)}")
    if not body.description.strip():
        raise InvalidException(message="description is required")

    target = await _resolve_target(session, user, ctx, body.user_id)
    now = datetime.now(timezone.utc)

    # Everything the reset forgives: the user-level long token windows of
    # this kind, the user's own limits and the default-plan fallback alike.
    # A user with no such window has nothing to forgive.
    windows = user_limit_windows(rlc, target.id, target) + default_plan_windows(
        rlc, target.id
    )
    counter_keys = [
        w.redis_key for w in windows if w.period == _RESET_PERIODS[body.kind]
    ]
    if not counter_keys:
        raise InvalidException(
            message=f"No {body.kind} budget is configured for this user"
        )

    status = await _reset_status(session, target.id, body.kind, now)
    if not is_platform_wide(user, ctx) and status.available <= 0:
        raise BadRequestException(message="Reset quota exhausted for this period")

    # The row lands before the counters are deleted: the counter rebuild
    # keys its cutoff off this table, so a rebuild that races the delete
    # must never see deleted counters without their cutoff.
    await GatewayRateLimitReset.create(
        session,
        GatewayRateLimitReset(
            user_id=target.id,
            kind=body.kind,
            reset_at=now,
            reset_by_user_id=user.id if user.id != target.id else None,
            description=body.description.strip(),
        ),
    )

    reader = _reader(request, cfg)
    await reader.client.delete(*counter_keys)

    return await _reset_status(session, target.id, body.kind, now)
