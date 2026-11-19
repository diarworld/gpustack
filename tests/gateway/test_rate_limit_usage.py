"""Redis-key reconstruction for the rate-limit usage reader.

The strings here are the plugin's documented key layout; a mismatch means the
endpoint would read keys the plugin never writes.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from gpustack.config.config import GatewayRateLimitConfig, GatewayRateLimitRedis
from gpustack.gateway.rate_limit import WEEK_IN_SECONDS, rate_limit_plugin_name
from gpustack.gateway.rate_limit_usage import (
    calendar_window_seconds,
    expected_windows,
    source_for,
)
from gpustack.security import gateway_consumer_identity


def _cfg(**overrides) -> GatewayRateLimitConfig:
    fields = dict(
        enabled=True,
        redis=GatewayRateLimitRedis(service_name="rate-limit-redis", password="pw"),
        timezone="UTC",
        default_rpm=10,
        default_tpm=60_000,
        default_daily_tokens=2_000_000,
        default_monthly_tokens=20_000_000,
    )
    fields.update(overrides)
    return GatewayRateLimitConfig(**fields)


@dataclass
class _KeyStub:
    """The ApiKey attributes the window reconstruction reads."""

    id: int
    access_key: Optional[str]
    user_id: int
    rpm_limit: Optional[int] = None
    tpm_limit: Optional[int] = None
    daily_token_limit: Optional[int] = None


def _shapes(windows):
    return [(w.kind, w.period, w.window_seconds, w.limit, w.redis_key) for w in windows]


def test_default_plan_keys():
    windows = expected_windows(_cfg(), 6, None, None, None)
    prefix = f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-6"
    assert _shapes(windows) == [
        ("query", "60s", 60, 10, f"{prefix}|q:60s"),
        ("token_rolling", "60s", 60, 60_000, f"{prefix}|t:60s"),
        ("token_calendar", "each_day", None, 2_000_000, f"{prefix}|t:each_day"),
        ("token_calendar", "each_month", None, 20_000_000, f"{prefix}|t:each_month"),
    ]


def test_fallback_adds_user_budget_windows():
    windows = expected_windows(_cfg(), 6, None, 100_000, 200_000)
    default_prefix = f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-6"
    user_prefix = f"{rate_limit_plugin_name}|user-6|consumer=gpustack-6"
    # Default-plan windows lead; the user budget combo appends. Both fallback
    # combos bucket on the captured gpustack-<id> segment, so their keys share
    # the identity even though the match rules differ.
    assert [w.redis_key for w in windows] == [
        f"{default_prefix}|q:60s",
        f"{default_prefix}|t:60s",
        f"{default_prefix}|t:each_day",
        f"{default_prefix}|t:each_month",
        f"{user_prefix}|t:604800s",
        f"{user_prefix}|t:each_month",
    ]
    by_key = {w.redis_key: w.limit for w in windows}
    assert by_key[f"{user_prefix}|t:604800s"] == 100_000
    assert by_key[f"{user_prefix}|t:each_month"] == 200_000
    assert windows[4].window_seconds == WEEK_IN_SECONDS


def test_explicit_key_windows():
    key = _KeyStub(id=42, access_key="ak-abc123", user_id=6, rpm_limit=5)
    windows = expected_windows(_cfg(), 6, key, 100_000, 200_000)
    identity = gateway_consumer_identity("ak-abc123", 6)
    # Pin the documented identity format: the consumer string ext-auth
    # injects for an API-key request is <access_key>.gpustack-<user_id>.
    assert identity == "ak-abc123.gpustack-6"
    prefix = f"{rate_limit_plugin_name}|key-42|consumer={identity}"
    assert _shapes(windows) == [
        ("query", "60s", 60, 5, f"{prefix}|q:60s"),
        # Unset key fields inherit the deployment default: the fallback is
        # suppressed for these requests, so without inheritance the guard
        # would silently vanish.
        ("token_rolling", "60s", 60, 60_000, f"{prefix}|t:60s"),
        # The owner's budgets are folded into the key combo.
        ("token_rolling", "604800s", WEEK_IN_SECONDS, 100_000, f"{prefix}|t:604800s"),
        ("token_calendar", "each_day", None, 2_000_000, f"{prefix}|t:each_day"),
        ("token_calendar", "each_month", None, 200_000, f"{prefix}|t:each_month"),
    ]
    # A non-fallback match suppresses the fallback group: no default-plan or
    # user budget keys exist for this request path.
    assert all(
        "default-plan|" not in w.redis_key and "user-6|" not in w.redis_key
        for w in windows
    )


def test_key_combo_monthly_has_no_default_fallback():
    """Monthly follows the owner's budget only -- unlike daily, it has no
    deployment default to inherit on key combos."""
    key = _KeyStub(id=7, access_key="ak-x", user_id=6, rpm_limit=5)
    windows = expected_windows(_cfg(), 6, key, None, None)
    assert [(w.kind, w.period) for w in windows] == [
        ("query", "60s"),
        ("token_rolling", "60s"),
        ("token_calendar", "each_day"),
    ]


def test_unlimited_key_falls_back_to_user_capture_identity():
    """A key without explicit limits matches no non-fallback combo: the
    fallback group governs, bucketed on the captured gpustack-<id> segment
    rather than the key's full consumer string."""
    key = _KeyStub(id=42, access_key="ak-abc123", user_id=6)
    windows = expected_windows(_cfg(), 6, key, None, None)
    prefix = f"{rate_limit_plugin_name}|default-plan|consumer=gpustack-6"
    assert [w.redis_key for w in windows] == [
        f"{prefix}|q:60s",
        f"{prefix}|t:60s",
        f"{prefix}|t:each_day",
        f"{prefix}|t:each_month",
    ]


def test_source_for():
    limited = _KeyStub(id=1, access_key="ak-1", user_id=6, rpm_limit=5)
    unlimited = _KeyStub(id=2, access_key="ak-2", user_id=6)
    assert source_for(limited, 100_000, 200_000) == "key"
    assert source_for(unlimited, 100_000, None) == "user+default"
    assert source_for(unlimited, None, 200_000) == "user+default"
    assert source_for(unlimited, None, None) == "default"
    assert source_for(None, None, None) == "default"


def test_calendar_window_seconds_utc():
    now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
    assert calendar_window_seconds("each_day", now, "UTC") == 43_200
    # Nov 19 12:00Z to Dec 1 00:00Z is 11.5 days.
    assert calendar_window_seconds("each_month", now, "UTC") == 993_600
    # Nov 19 12:00Z to Jan 1 00:00Z is 42.5 days.
    assert calendar_window_seconds("each_year", now, "UTC") == 3_672_000


def test_calendar_window_seconds_honours_timezone():
    now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
    # 12:00Z is 13:00 in Berlin (UTC+1 in November), so midnight there is
    # 11 hours away.
    assert calendar_window_seconds("each_day", now, "Europe/Berlin") == 39_600


def test_calendar_window_seconds_rejects_rolling_periods():
    import pytest

    with pytest.raises(ValueError):
        calendar_window_seconds(
            "60s", datetime(2026, 11, 19, tzinfo=timezone.utc), "UTC"
        )


def test_no_windows_when_nothing_configured():
    rlc = _cfg(
        default_rpm=None,
        default_tpm=None,
        default_daily_tokens=None,
        default_monthly_tokens=None,
    )
    assert expected_windows(rlc, 6, None, None, None) == []


def test_unset_defaults_skip_their_windows():
    rlc = _cfg(default_tpm=None, default_daily_tokens=None, default_monthly_tokens=None)
    windows = expected_windows(rlc, 6, None, None, None)
    assert [(w.kind, w.period) for w in windows] == [("query", "60s")]
