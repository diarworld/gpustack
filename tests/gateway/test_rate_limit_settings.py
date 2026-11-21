"""The editable deployment defaults (``gateway_rate_limit_settings``).

Covers the three pieces everything downstream leans on: the settings-row
seed, the layering of the row over the config file, and the free-window
schedule the server re-derives for the Usage badge (the plugin enforces
its own copy of the same semantics, covered by the plugin's Go tests).
"""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from gpustack.config.config import GatewayRateLimitConfig
from gpustack.gateway.rate_limit_settings import (
    FREE_WINDOW_DEFAULT_DAYS,
    FREE_WINDOW_DEFAULT_END,
    FREE_WINDOW_DEFAULT_START,
    FreeWindow,
    effective_rlc,
    free_window_from,
    get_or_create_settings,
)
from gpustack.schemas.gateway_rate_limit_settings import GatewayRateLimitSettings


def _rlc(**kwargs) -> GatewayRateLimitConfig:
    return GatewayRateLimitConfig(enabled=True, **kwargs)


class FakeExecResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class FakeSettingsSession:
    """Returns the pre-loaded row from the settings select; records writes."""

    def __init__(self, row=None):
        self._row = row
        self.added = []
        self.committed = False

    async def exec(self, statement):
        return FakeExecResult(self._row)

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.committed = True

    async def refresh(self, obj):
        obj.id = 1


class TestGetOrCreateSettings:
    @pytest.mark.asyncio
    async def test_seeds_the_deployment_defaults_once(self):
        """A fresh deployment must get the free schedule on (evenings,
        nights and weekends in UTC) with NULL limits -- the config file
        stays the limit source until the first UI save."""
        session = FakeSettingsSession(row=None)
        row = await get_or_create_settings(session, cfg=SimpleNamespace())

        assert row.default_rpm is None
        assert row.default_tpm is None
        assert row.default_monthly_tokens is None
        assert row.default_concurrency is None
        assert row.free_enabled is True
        assert row.free_start == FREE_WINDOW_DEFAULT_START
        assert row.free_end == FREE_WINDOW_DEFAULT_END
        assert row.free_days == FREE_WINDOW_DEFAULT_DAYS
        assert session.committed is True

    @pytest.mark.asyncio
    async def test_existing_row_is_returned_untouched(self):
        row = GatewayRateLimitSettings(id=1, free_enabled=False, free_days=["sun"])
        session = FakeSettingsSession(row=row)
        got = await get_or_create_settings(session, cfg=SimpleNamespace())
        assert got is row
        assert session.added == []


class TestEffectiveRlc:
    def test_null_fields_inherit_the_config_file(self):
        rlc = _rlc(default_rpm=30, default_concurrency=2)
        row = GatewayRateLimitSettings(id=1)
        got = effective_rlc(rlc, row)
        assert got.default_rpm == 30
        assert got.default_concurrency == 2

    def test_explicit_row_values_win(self):
        rlc = _rlc(default_rpm=30, default_tpm=1_000, default_concurrency=2)
        row = GatewayRateLimitSettings(
            id=1, default_rpm=120, default_tpm=500_000, default_concurrency=0
        )
        got = effective_rlc(rlc, row)
        assert got.default_rpm == 120
        assert got.default_tpm == 500_000
        # An explicit 0 turns the cap off; None would have inherited the 2.
        assert got.default_concurrency == 0


class TestFreeWindowIsActive:
    def _free(self, **kwargs) -> FreeWindow:
        return FreeWindow(
            enabled=kwargs.pop("enabled", True),
            start=kwargs.pop("start", "15:00"),
            end=kwargs.pop("end", "06:00"),
            days=kwargs.pop("days", []),
        )

    def test_disabled_is_never_active(self):
        fw = self._free(enabled=False, days=["sat"])
        # 2026-11-21 is a Saturday.
        now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
        assert fw.is_active(now, "UTC") is False

    def test_wrapping_window_covers_both_sides_of_midnight(self):
        fw = self._free()
        before = datetime(2026, 11, 18, 14, 59, tzinfo=timezone.utc)
        inside_evening = datetime(2026, 11, 18, 15, 0, tzinfo=timezone.utc)
        inside_night = datetime(2026, 11, 19, 0, 0, tzinfo=timezone.utc)
        last_minute = datetime(2026, 11, 19, 5, 59, tzinfo=timezone.utc)
        after = datetime(2026, 11, 19, 6, 0, tzinfo=timezone.utc)
        assert fw.is_active(before, "UTC") is False
        assert fw.is_active(inside_evening, "UTC") is True
        assert fw.is_active(inside_night, "UTC") is True
        assert fw.is_active(last_minute, "UTC") is True
        assert fw.is_active(after, "UTC") is False

    def test_free_day_covers_the_whole_day(self):
        fw = self._free(days=["sat", "sun"])
        # Saturday 12:00 sits outside the 15:00-06:00 window, the free day
        # still suspends everything.
        saturday_noon = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
        monday_noon = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
        assert fw.is_active(saturday_noon, "UTC") is True
        assert fw.is_active(monday_noon, "UTC") is False

    def test_evaluated_in_the_deployment_timezone(self):
        """The same instant can sit on different sides of the boundary in
        different zones: 14:00 UTC is outside 15:00-06:00, but Moscow
        (UTC+3, no DST) is already at 17:00 -- inside. The zone the
        schedule is judged in decides."""
        fw = self._free()
        instant = datetime(2026, 11, 18, 14, 0, tzinfo=timezone.utc)
        assert fw.is_active(instant, "UTC") is False
        assert fw.is_active(instant, "Europe/Moscow") is True

    def test_garbled_times_activate_nothing(self):
        fw = FreeWindow(enabled=True, start="bad", end="06:00", days=[])
        now = datetime(2026, 11, 18, 20, 0, tzinfo=timezone.utc)
        assert fw.is_active(now, "UTC") is False

    def test_plugin_config_renders_the_plugin_shape(self):
        fw = self._free(days=["sat"])
        assert fw.plugin_config() == {
            "enabled": True,
            "start": "15:00",
            "end": "06:00",
            "days": ["sat"],
        }

    def test_free_window_from_row(self):
        row = GatewayRateLimitSettings(
            id=1,
            free_enabled=True,
            free_start="01:00",
            free_end="02:00",
            free_days=["mon"],
        )
        fw = free_window_from(row)
        assert fw == FreeWindow(enabled=True, start="01:00", end="02:00", days=["mon"])
