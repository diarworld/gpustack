"""Deployment-wide rate-limit defaults for the Usage page (``GET`` /
``PUT /rate-limit-settings``).

The singleton row layered over the ``gateway_rate_limit`` config file is
what the gateway's reconciler publishes and the Usage endpoints display;
this router is the admin's handle on it. A PUT only writes the row --
the reconciler's watch on the table fans the change out to the gateway
CR at its next pass, so limits move without a restart.

Admin-only by mount (``routes.py`` admin_routers): these are
deployment-wide defaults, not per-Org data.
"""

from fastapi import APIRouter, Request

from gpustack.config.config import Config
from gpustack.gateway.rate_limit_settings import get_or_create_settings
from gpustack.schemas.gateway_rate_limit_settings import (
    FreeWindowPublic,
    GatewayRateLimitSettingsPublic,
    GatewayRateLimitSettingsUpdate,
)
from gpustack.server.deps import SessionDep

router = APIRouter()


def _public(row) -> GatewayRateLimitSettingsPublic:
    return GatewayRateLimitSettingsPublic(
        default_rpm=row.default_rpm,
        default_tpm=row.default_tpm,
        default_monthly_tokens=row.default_monthly_tokens,
        default_concurrency=row.default_concurrency,
        free=FreeWindowPublic(
            enabled=row.free_enabled,
            start=row.free_start,
            end=row.free_end,
            days=list(row.free_days or []),
        ),
    )


@router.get("", response_model=GatewayRateLimitSettingsPublic)
async def get_rate_limit_settings(session: SessionDep, request: Request):
    """The deployment defaults and free schedule as currently stored."""
    cfg: Config = request.app.state.server_config
    row = await get_or_create_settings(session, cfg)
    return _public(row)


@router.put("", response_model=GatewayRateLimitSettingsPublic)
async def update_rate_limit_settings(
    body: GatewayRateLimitSettingsUpdate, session: SessionDep, request: Request
):
    """Replace the deployment defaults and free schedule.

    A full-replace payload: ``null`` on a limit field returns that
    default to the config-file value. The row write is the whole
    operation -- the reconciler picks the change up from the table's
    event stream and re-renders the gateway CR.
    """
    cfg: Config = request.app.state.server_config
    row = await get_or_create_settings(session, cfg)
    row.default_rpm = body.default_rpm
    row.default_tpm = body.default_tpm
    row.default_monthly_tokens = body.default_monthly_tokens
    row.default_concurrency = body.default_concurrency
    row.free_enabled = body.free.enabled
    row.free_start = body.free.start
    row.free_end = body.free.end
    row.free_days = list(body.free.days)
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return _public(row)
