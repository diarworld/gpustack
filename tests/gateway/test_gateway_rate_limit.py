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
        """Enabling with every default off (``default_concurrency=0``) is how
        the reconciler takes ownership of the combinations before any default
        exists: the plugin must be on the gateway with its base fields, but
        not declare an empty combinations list the reconciler would have to
        diff against."""
        result = build(enabled=True, default_concurrency=0)
        assert result is not None
        name, spec = result
        assert name == rate_limit_plugin_name
        assert "limit_combinations" not in spec.defaultConfig
        assert spec.defaultConfig["rule_name"] == rate_limit_plugin_name

    def test_concurrency_default_alone_publishes_one_fallback_combo(self):
        """The concurrency cap defaults to 1, so a freshly enabled
        deployment enforces something even with every other axis unset:
        the fallback combo carries only the concurrency block."""
        _, spec = build(enabled=True)
        (combo,) = spec.defaultConfig["limit_combinations"]
        assert combo["name"] == "default-plan"
        assert combo["is_fallback"] is True
        assert combo["concurrency_limits"] == {"default": 1}
        assert "query_limits" not in combo
        assert "token_limits" not in combo
        assert "token_quota" not in combo

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
        assert combo["token_quota"] == {"each_month": 5_000_000}

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
        # The plugin names the McpBridge registry, not the raw Service
        # FQDN: this gateway routes through McpBridge registries, and a
        # raw FQDN has no Envoy cluster behind it.
        assert redis["service_name"] == "gpustack-rate-limit-redis.dns"
        assert redis["service_port"] == 6379
        assert redis["password"] == "secret"
        assert redis["database"] == 2

    def test_no_redis_section_means_local_mode(self):
        _, spec = build(enabled=True, default_rpm=60)
        assert "redis" not in spec.defaultConfig


class TestRateLimitRedisRegistry:
    def test_registry_carries_the_service_as_a_dns_domain(self):
        from gpustack.gateway.rate_limit import rate_limit_redis_registry

        cfg = make_cfg(
            GatewayRateLimitConfig(
                enabled=True,
                redis=GatewayRateLimitRedis(
                    service_name="rate-limit-redis.gpustack.svc.cluster.local"
                ),
            )
        )
        registry = rate_limit_redis_registry(cfg, "gpustack")
        assert registry is not None
        assert registry.name == "gpustack-rate-limit-redis"
        assert registry.type == "dns"
        assert registry.domain == "rate-limit-redis.gpustack.svc.cluster.local"
        assert registry.port == 6379
        assert registry.protocol == "tcp"

    def test_bare_service_name_is_namespaced_for_strict_dns(self):
        """A single-label Service name does not resolve in Envoy's
        STRICT_DNS; the registry domain must carry the namespace and stop
        at ``.svc`` so the pod's search list completes the cluster domain."""
        from gpustack.gateway.rate_limit import rate_limit_redis_registry

        cfg = make_cfg(
            GatewayRateLimitConfig(
                enabled=True,
                redis=GatewayRateLimitRedis(service_name="redis"),
            )
        )
        registry = rate_limit_redis_registry(cfg, "gpustack")
        assert registry.domain == "redis.gpustack.svc"

    def test_none_without_redis_or_when_disabled(self):
        from gpustack.gateway.rate_limit import rate_limit_redis_registry

        assert (
            rate_limit_redis_registry(
                make_cfg(GatewayRateLimitConfig(enabled=True)), "gpustack"
            )
            is None
        )
        cfg = make_cfg(
            GatewayRateLimitConfig(
                enabled=False,
                redis=GatewayRateLimitRedis(service_name="redis"),
            )
        )
        assert rate_limit_redis_registry(cfg, "gpustack") is None

    def test_timezone_anchors_calendar_budgets(self):
        _, spec = build(
            enabled=True, default_monthly_tokens=1, timezone="Asia/Shanghai"
        )
        assert spec.defaultConfig["timezone"] == "Asia/Shanghai"

    def test_rejection_code_and_msg_stay_absent_by_default(self):
        """The plugin's own rejection texts name the limit axis that tripped
        and always answer 429; an operator-set value here would override
        them, so the generated config carries neither field by default."""
        _, spec = build(enabled=True)
        assert "rejected_code" not in spec.defaultConfig
        assert "rejected_msg" not in spec.defaultConfig
        assert spec.defaultConfig["show_limit_quota_header"] is True

    def test_operator_pinned_rejection_fields_reach_the_config(self):
        _, spec = build(
            enabled=True,
            rejected_code=503,
            rejected_msg="Back off",
        )
        assert spec.defaultConfig["rejected_code"] == 503
        assert spec.defaultConfig["rejected_msg"] == "Back off"

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
            "default_monthly_tokens",
        ):
            with pytest.raises(ValidationError):
                GatewayRateLimitConfig(**{field: 0})

    def test_unknown_fields_are_refused(self):
        with pytest.raises(ValidationError):
            GatewayRateLimitConfig(enable_on_path_suffix=["*"])
