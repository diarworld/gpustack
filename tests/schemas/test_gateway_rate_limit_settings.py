"""Validation on the ``gateway_rate_limit_settings`` models.

The values here render straight into the gateway plugin's config JSON, so
anything the plugin would refuse must be rejected before it reaches the
database.
"""

import pytest
from pydantic import ValidationError

from gpustack.schemas.gateway_rate_limit_settings import (
    FREE_DAYS,
    FreeWindowUpdate,
    GatewayRateLimitSettingsUpdate,
)


class TestFreeWindowUpdate:
    def test_defaults_match_the_deployment_seed(self):
        fw = FreeWindowUpdate()
        assert fw.enabled is False
        assert fw.start == "15:00"
        assert fw.end == "06:00"
        assert fw.days == []

    def test_window_may_wrap_past_midnight(self):
        fw = FreeWindowUpdate(enabled=True, start="15:00", end="06:00")
        assert fw.start == "15:00"

    @pytest.mark.parametrize("bad", ["1500", "25:00", "6:00:00", "", "ab:cd"])
    def test_garbled_times_are_rejected(self, bad):
        with pytest.raises(ValidationError):
            FreeWindowUpdate(enabled=True, start=bad, end="06:00")

    def test_equal_bounds_are_rejected_only_when_enabled(self):
        """A disabled schedule is inert, so its stored bounds are never
        evaluated -- but a typo must not be storable for the moment the
        admin flips the switch on."""
        with pytest.raises(ValidationError):
            FreeWindowUpdate(enabled=True, start="06:00", end="06:00")

    def test_days_are_normalized_and_canonicalized(self):
        fw = FreeWindowUpdate(enabled=True, days=["SUN", " sat ", "sun"])
        # Saturday before Sunday, duplicates dropped: the list renders
        # straight into the plugin config.
        assert fw.days == ["sat", "sun"]

    def test_unknown_days_are_rejected(self):
        with pytest.raises(ValidationError):
            FreeWindowUpdate(enabled=True, days=["sat", "funday"])


class TestGatewayRateLimitSettingsUpdate:
    def test_null_limits_mean_inherit(self):
        body = GatewayRateLimitSettingsUpdate()
        assert body.default_rpm is None
        assert body.default_tpm is None
        assert body.default_monthly_tokens is None
        assert body.default_concurrency is None
        assert body.free == FreeWindowUpdate()

    def test_zero_concurrency_is_an_explicit_off(self):
        body = GatewayRateLimitSettingsUpdate(default_concurrency=0)
        assert body.default_concurrency == 0

    @pytest.mark.parametrize(
        "field", ["default_rpm", "default_tpm", "default_monthly_tokens"]
    )
    def test_non_positive_limits_are_rejected(self, field):
        """A zero or negative default would either never bind or reject
        every request; ``None`` is the only way to say 'no default'."""
        with pytest.raises(ValidationError):
            GatewayRateLimitSettingsUpdate(**{field: 0})

    def test_free_payload_validates_through(self):
        body = GatewayRateLimitSettingsUpdate(
            default_rpm=100,
            free={
                "enabled": True,
                "start": "15:00",
                "end": "06:00",
                "days": ["sat", "sun"],
            },
        )
        assert body.free.days == ["sat", "sun"]
        assert list(FREE_DAYS) == [
            "mon",
            "tue",
            "wed",
            "thu",
            "fri",
            "sat",
            "sun",
        ]
