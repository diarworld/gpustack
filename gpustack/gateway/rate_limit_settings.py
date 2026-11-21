"""The editable deployment-wide rate-limit defaults and free schedule.

The ``gateway_rate_limit`` config file seeds a fresh deployment; from
then on the ``gateway_rate_limit_settings`` singleton row is the source
of truth for the default limits (``NULL`` inherits the config value) and
carries the free-window schedule. Everything that reads the defaults --
the gateway reconciler publishing the plugin CR, the Usage endpoints
displaying them -- resolves through :func:`effective_rate_limit`, so a
change made in the UI lands everywhere at the next reconcile pass with
no restart.

The free schedule mirrors the plugin's semantics exactly (see the
plugin's ``free_windows``): on a listed day everything is free, and on
any day the daily window -- which may wrap past midnight -- is free.
Wall-clock values are UTC, the timezone the deployed plugin loads.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple

from sqlalchemy.exc import IntegrityError
from sqlmodel import select
from zoneinfo import ZoneInfo

from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.schemas.gateway_rate_limit_settings import (
    GatewayRateLimitSettings,
)

logger = logging.getLogger(__name__)

SETTINGS_ROW_ID = 1

# The schedule an admin sees on a fresh deployment: Moscow's evenings,
# nights and weekends in UTC -- 15:00-06:00 UTC is 18:00-09:00 in
# Europe/Moscow, which has no DST.
FREE_WINDOW_DEFAULT_START = "15:00"
FREE_WINDOW_DEFAULT_END = "06:00"
FREE_WINDOW_DEFAULT_DAYS: List[str] = ["sat", "sun"]

# Monday-first day tokens, indexed by ``date.weekday()``.
DAY_NAMES: Tuple[str, ...] = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


@dataclass
class FreeWindow:
    """The free schedule the plugin enforces and the Usage page shows."""

    enabled: bool
    start: str
    end: str
    days: List[str] = field(default_factory=list)

    def is_active(self, now: datetime, timezone_name: str) -> bool:
        """Whether ``now`` falls inside the schedule, in the deployment's
        rate-limit timezone -- the same zone that anchors the plugin's
        calendar quotas, so a free window and a budget boundary cannot
        disagree about when a day starts."""
        if not self.enabled:
            return False
        try:
            tz = ZoneInfo(timezone_name)
        except Exception:
            tz = ZoneInfo("UTC")
        local = now.astimezone(tz)
        days = {d.lower() for d in self.days}
        # Indexed off weekday() rather than strftime('%a'): the former is
        # locale-independent, the latter is not.
        if DAY_NAMES[local.weekday()] in days:
            return True
        minutes = local.hour * 60 + local.minute
        start = _parse_hhmm(self.start)
        end = _parse_hhmm(self.end)
        if start is None or end is None or start == end:
            return False
        if start < end:
            return start <= minutes < end
        # Wrap past midnight: [start, 24:00) plus [00:00, end).
        return minutes >= start or minutes < end

    def plugin_config(self) -> dict:
        """The block rendered into the plugin's ``defaultConfig``; the
        shape (and the day tokens) match the plugin's FreeWindowConfig."""
        return {
            "enabled": self.enabled,
            "start": self.start,
            "end": self.end,
            "days": list(self.days),
        }


def _parse_hhmm(value: str) -> Optional[int]:
    try:
        hours, minutes = value.split(":")
        return int(hours) * 60 + int(minutes)
    except (ValueError, AttributeError):
        return None


def _seed_row(cfg: Config) -> GatewayRateLimitSettings:
    return GatewayRateLimitSettings(
        id=SETTINGS_ROW_ID,
        # NULL limits: a fresh deployment inherits the config file, so the
        # first UI save starts from what the operator deployed with.
        default_rpm=None,
        default_tpm=None,
        default_monthly_tokens=None,
        default_concurrency=None,
        free_enabled=True,
        free_start=FREE_WINDOW_DEFAULT_START,
        free_end=FREE_WINDOW_DEFAULT_END,
        free_days=list(FREE_WINDOW_DEFAULT_DAYS),
    )


async def get_or_create_settings(session, cfg: Config) -> GatewayRateLimitSettings:
    """The settings row, seeded from the deployment defaults on first
    read. Two servers racing the seed resolve on the fixed primary key:
    the loser re-reads the winner's row."""
    row = (
        await session.exec(
            select(GatewayRateLimitSettings).where(
                GatewayRateLimitSettings.id == SETTINGS_ROW_ID
            )
        )
    ).first()
    if row is not None:
        return row
    row = _seed_row(cfg)
    session.add(row)
    try:
        await session.commit()
        await session.refresh(row)
        return row
    except IntegrityError:
        await session.rollback()
        return (
            await session.exec(
                select(GatewayRateLimitSettings).where(
                    GatewayRateLimitSettings.id == SETTINGS_ROW_ID
                )
            )
        ).first()


def effective_rlc(
    rlc: GatewayRateLimitConfig, row: GatewayRateLimitSettings
) -> GatewayRateLimitConfig:
    """The config with the row's explicit defaults layered over it: the
    validated pydantic model everything downstream already reads, so
    callers cannot tell a UI edit from a config-file value."""
    updates = {}
    for name in (
        "default_rpm",
        "default_tpm",
        "default_monthly_tokens",
        "default_concurrency",
    ):
        value = getattr(row, name)
        if value is not None:
            updates[name] = value
    return rlc.model_copy(update=updates)


def free_window_from(row: GatewayRateLimitSettings) -> FreeWindow:
    return FreeWindow(
        enabled=row.free_enabled,
        start=row.free_start,
        end=row.free_end,
        days=list(row.free_days or []),
    )


async def effective_rate_limit(
    session, cfg: Config
) -> Tuple[GatewayRateLimitConfig, FreeWindow]:
    """The deployment defaults and free schedule an admin currently
    runs under: the config file with the settings row layered over it.
    Fails back to the raw config when the row cannot be read -- the
    gateway enforcing the published CR must not go dark over a UI
    table hiccup."""
    rlc = cfg.gateway_rate_limit
    if rlc is None:
        return rlc, FreeWindow(enabled=False, start="00:00", end="00:00", days=[])
    try:
        row = await get_or_create_settings(session, cfg)
    except Exception as e:
        logger.error(
            "Gateway rate limit: reading the settings row failed, "
            "falling back to the config file: %s",
            e,
        )
        return rlc, FreeWindow(enabled=False, start="00:00", end="00:00", days=[])
    return effective_rlc(rlc, row), free_window_from(row)
