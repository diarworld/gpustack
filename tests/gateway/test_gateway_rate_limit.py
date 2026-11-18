import pytest
from unittest.mock import MagicMock
from pydantic import ValidationError

from gpustack.config.config import (
    GatewayPluginEntry,
    GatewayRateLimitConfig,
    GatewayRateLimitRedis,
)
from gpustack.gateway import rate_limit_plugin, rate_limit_plugin_name


def make_cfg(gateway_rate_limit: GatewayRateLimitConfig) -> MagicMock:
    cfg = MagicMock()
    cfg.gateway_plugin_server_url = "http://127.0.0.1"
    cfg.gateway_plugin = {}
    cfg.gateway_rate_limit = gateway_rate_limit
    return cfg


def build(**kwargs):
    return rate_limit_plugin(cfg=make_cfg(GatewayRateLimitConfig(**kwargs)))


class TestRateLimitPlugin:
    def test_disabled_by_default_publishes_nothing(self):
        assert rate_limit_plugin(cfg=make_cfg(GatewayRateLimitConfig())) is None

    def test_enabled_without_limits_publishes_a_passthrough_base(self):
        """Enabling with no limits configured is how the reconciler takes
        ownership of the combinations before any default exists: the plugin
        must be on the gateway with its base fields, but not declare an empty
        combinations list the reconciler would have to diff against."""
        result = build(enabled=True)
        assert result is not None
        name, spec = result
        assert name == rate_limit_plugin_name
        assert "limit_combinations" not in spec.defaultConfig
        assert spec.defaultConfig["rule_name"] == rate_limit_plugin_name

    def test_sits_in_the_reserved_slot_after_ext_auth_and_ai_statistics(self):
        """Priority 600: ext-auth (360) has injected the consumer identity by
        the time this runs in the request path, and ai-statistics (900) has
        written ai_log by the time the response-phase hook reads it."""
        _, spec = build(enabled=True)
        assert spec.phase == "UNSPECIFIED_PHASE"
        assert spec.priority == 600

    def test_defaults_become_one_fallback_combo_bucketed_per_user(self):
        _, spec = build(
            enabled=True,
            default_rpm=60,
            default_tpm=100_000,
            default_daily_tokens=500_000,
            default_monthly_tokens=5_000_000,
        )

        combos = spec.defaultConfig["limit_combinations"]
        assert len(combos) == 1
        combo = combos[0]
        assert combo["name"] == "default-plan"
        assert combo["is_fallback"] is True
        # The per-user capture is what keeps a user's API-key and UI traffic
        # in one bucket; the two consumer shapes must both collapse onto the
        # same captured segment.
        (match,) = combo["match"]
        assert match["source"] == "consumer"
        pattern = match["value"]
        assert pattern.startswith("regexp_capture:")
        import re

        expression = pattern.removeprefix("regexp_capture:")
        # The per-user capture is what keeps a user's API-key and UI traffic
        # in one bucket: both consumer shapes must reduce to the same
        # captured segment.
        assert re.fullmatch(expression, "ak-123.gpustack-42").group(1) == "gpustack-42"
        assert re.fullmatch(expression, "gpustack-42").group(1) == "gpustack-42"

        assert combo["query_limits"] == {"per_minute": 60}
        assert combo["token_limits"] == {"per_minute": 100_000}
        assert combo["token_quota"] == {
            "each_day": 500_000,
            "each_month": 5_000_000,
        }

    def test_unset_limits_do_not_declare_windows(self):
        _, spec = build(enabled=True, default_rpm=30)
        (combo,) = spec.defaultConfig["limit_combinations"]
        assert combo["query_limits"] == {"per_minute": 30}
        assert "token_limits" not in combo
        assert "token_quota" not in combo

    def test_redis_backend_reaches_the_config(self):
        _, spec = build(
            enabled=True,
            default_rpm=60,
            redis=GatewayRateLimitRedis.model_validate(
                {
                    "service_name": "redis.gpustack-system.svc.cluster.local",
                    "password": "secret",
                    "database": 2,
                }
            ),
        )
        redis = spec.defaultConfig["redis"]
        assert redis["service_name"] == "redis.gpustack-system.svc.cluster.local"
        assert redis["service_port"] == 6379
        assert redis["password"] == "secret"
        assert redis["database"] == 2

    def test_no_redis_section_means_local_mode(self):
        _, spec = build(enabled=True, default_rpm=60)
        assert "redis" not in spec.defaultConfig

    def test_timezone_anchors_calendar_budgets(self):
        _, spec = build(enabled=True, default_daily_tokens=1, timezone="Asia/Shanghai")
        assert spec.defaultConfig["timezone"] == "Asia/Shanghai"

    def test_rejection_defaults_are_openai_shaped(self):
        _, spec = build(enabled=True)
        assert spec.defaultConfig["rejected_code"] == 429
        assert spec.defaultConfig["rejected_msg"] == "Too many requests"
        assert spec.defaultConfig["show_limit_quota_header"] is True

    def test_module_url_resolves_from_the_plugin_manifest(self):
        from gpustack.gateway.plugins import (
            get_plugin_url_with_name_and_version,
            supported_plugins,
        )

        version = next(
            p.version for p in supported_plugins if p.name == rate_limit_plugin_name
        )
        _, spec = build(enabled=True)
        assert spec.url == get_plugin_url_with_name_and_version(
            rate_limit_plugin_name, version, None
        )

    def test_operator_module_override_wins(self):
        cfg = make_cfg(GatewayRateLimitConfig(enabled=True))
        cfg.gateway_plugin = {
            rate_limit_plugin_name: GatewayPluginEntry.model_validate(
                {"url": "oci://registry.example/rate-limit:9", "sha256": "ab"}
            )
        }
        _, spec = rate_limit_plugin(cfg=cfg)
        assert spec.url == "oci://registry.example/rate-limit:9"
        assert spec.sha256 == "ab"

    def test_plugin_config_passthrough_overrides_base_fields(self):
        """The escape hatch for plugin knobs the server does not model; it
        deliberately wins over the generated base so a knob the server has not
        picked up yet is still reachable."""
        _, spec = build(
            enabled=True,
            default_rpm=60,
            plugin_config={"rejected_msg": "Quota exceeded for today"},
        )
        assert spec.defaultConfig["rejected_msg"] == "Quota exceeded for today"
        assert "limit_combinations" in spec.defaultConfig


class TestGatewayRateLimitConfigValidation:
    def test_rejected_code_must_be_a_valid_status(self):
        with pytest.raises(ValidationError):
            GatewayRateLimitConfig(rejected_code=99)
        with pytest.raises(ValidationError):
            GatewayRateLimitConfig(rejected_code=600)

    def test_limits_must_be_positive(self):
        for field in (
            "default_rpm",
            "default_tpm",
            "default_daily_tokens",
            "default_monthly_tokens",
        ):
            with pytest.raises(ValidationError):
                GatewayRateLimitConfig(**{field: 0})

    def test_unknown_fields_are_refused(self):
        with pytest.raises(ValidationError):
            GatewayRateLimitConfig(enable_on_path_suffix=["*"])
