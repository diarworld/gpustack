import re

import pytest
from unittest.mock import MagicMock

from gpustack.config.config import GatewayRateLimitConfig
from gpustack.gateway.rate_limit import WEEK_IN_SECONDS
from gpustack.server.gateway_rate_limit_reconciler import (
    fit_combinations,
    reconcile_spec_diff,
    user_limits_combination,
)


def make_cfg(**kwargs):
    cfg = MagicMock()
    cfg.gateway_rate_limit = GatewayRateLimitConfig(enabled=True, **kwargs)
    # A real dict: a MagicMock here would leak a MagicMock image_pull_policy
    # into plugin_spec_overrides and collide with the spec's own value.
    cfg.gateway_plugin = {}
    return cfg


def make_user(id=42, **limits):
    user = MagicMock()
    user.id = id
    user.rpm_limit = limits.get("rpm_limit")
    user.tpm_limit = limits.get("tpm_limit")
    user.weekly_token_limit = limits.get("weekly_token_limit")
    user.monthly_token_limit = limits.get("monthly_token_limit")
    # Explicit: a MagicMock attribute would auto-create a truthy is_admin
    # and silently turn every fixture user into an exempt admin.
    user.is_admin = limits.get("is_admin", False)
    user.model_concurrency_limits = limits.get("model_concurrency_limits")
    return user


class TestUserLimitsCombination:
    def test_buckets_both_consumer_shapes_on_the_user_segment(self):
        combo = user_limits_combination(
            make_user(), make_cfg(default_rpm=10, default_monthly_tokens=1_000_000)
        )
        (match,) = combo["match"]
        expression = match["value"].removeprefix("regexp_capture:")
        assert re.fullmatch(expression, "ak-123.gpustack-42").group(1) == "gpustack-42"
        assert re.fullmatch(expression, "gpustack-42").group(1) == "gpustack-42"
        # A different user's traffic must not land in this bucket.
        assert re.fullmatch(expression, "gpustack-43") is None

    def test_is_non_fallback_so_it_binds_every_request(self):
        """A user cap must bind every request the account sends: non-fallback
        combos AND, fallback combos are suppressed the moment any of them
        matches."""
        combo = user_limits_combination(
            make_user(), make_cfg(default_rpm=10, default_monthly_tokens=1_000_000)
        )
        assert "is_fallback" not in combo

    def test_unset_fields_inherit_the_deployment_default(self):
        combo = user_limits_combination(
            make_user(),
            make_cfg(
                default_rpm=60,
                default_tpm=100_000,
                default_monthly_tokens=5_000_000,
            ),
        )
        assert combo["name"] == "user-42"
        assert combo["query_limits"] == {"per_minute": 60}
        assert combo["token_limits"] == {"per_minute": 100_000}
        assert combo["token_quota"] == {"each_month": 5_000_000}

    def test_explicit_fields_win_over_defaults(self):
        user = make_user(
            rpm_limit=30,
            tpm_limit=200_000,
            weekly_token_limit=5_000,
            monthly_token_limit=50_000_000,
        )
        combo = user_limits_combination(user, make_cfg(default_rpm=60))
        assert combo["query_limits"] == {"per_minute": 30}
        assert combo["token_limits"] == {
            "per_minute": 200_000,
            "per_custom": 5_000,
            "custom_window_seconds": WEEK_IN_SECONDS,
        }
        assert combo["token_quota"] == {"each_month": 50_000_000}

    def test_nothing_configured_anywhere_renders_nothing(self):
        # With the concurrency cap off (0) and no rate fields anywhere, the
        # combo renders nothing; the concurrency default of 1 is what keeps
        # a combo alive in every other configuration.
        assert (
            user_limits_combination(make_user(), make_cfg(default_concurrency=0))
            is None
        )

    def test_concurrency_default_inherits_into_the_user_combo(self):
        """The user combo suppresses the fallback plan, so the deployment
        concurrency default must be restated on it -- inheriting by omission
        would leave key-holding users uncapped."""
        combo = user_limits_combination(make_user(), make_cfg())
        assert combo["concurrency_limits"] == {"default": 1}

    def test_per_model_overrides_land_in_the_models_map(self):
        user = make_user(model_concurrency_limits={"glm-5.3-flash": 4, "free": 0})
        combo = user_limits_combination(user, make_cfg())
        assert combo["concurrency_limits"] == {
            "default": 1,
            "models": {"glm-5.3-flash": 4, "free": 0},
        }

    def test_zero_default_with_overrides_caps_only_named_models(self):
        """``default_concurrency=0`` turns the deployment cap off; a user's
        per-model map must still cap exactly the models it names."""
        user = make_user(model_concurrency_limits={"glm-5.3-flash": 2})
        combo = user_limits_combination(user, make_cfg(default_concurrency=0))
        assert combo["concurrency_limits"] == {"models": {"glm-5.3-flash": 2}}

    def test_admins_are_exempt_by_omission(self):
        """A non-fallback match suppresses the fallback plan, so an admin
        combo without the concurrency block exempts the account entirely --
        even when per-model overrides linger on the row."""
        user = make_user(is_admin=True, model_concurrency_limits={"glm-5.3-flash": 4})
        combo = user_limits_combination(user, make_cfg(default_rpm=10))
        assert "concurrency_limits" not in combo

    def test_concurrency_alone_keeps_the_combo_alive(self):
        """A user with no rate fields or budgets must still get a combo once
        the deployment concurrency cap is on: their requests would otherwise
        fall through to the fallback plan and lose the per-user bucket."""
        combo = user_limits_combination(make_user(), make_cfg())
        assert combo["name"] == "user-42"
        assert "query_limits" not in combo
        assert "token_limits" not in combo
        assert "token_quota" not in combo
        assert combo["concurrency_limits"] == {"default": 1}


class TestFitCombinations:
    def test_everything_is_kept_when_it_fits(self):
        first = {"name": "user-1", "match": []}
        second = {"name": "user-2", "match": [{"source": "consumer", "value": "x"}]}
        # The kept list comes back in input order.
        assert fit_combinations([first, second], budget=10_000) == [first, second]

    def test_over_budget_combos_are_dropped_in_input_order(self):
        big = {"name": "user-1", "match": [{"source": "consumer", "value": "x"}]}
        small = {"name": "user-2", "match": []}
        kept = fit_combinations([big, small], budget=json_small(small))
        assert kept == [small]

    def test_zero_budget_keeps_nothing(self):
        assert fit_combinations([{"name": "user-1", "match": []}], budget=0) == []


def json_small(obj):
    import json

    return len(json.dumps(obj)) + 8


class TestReconcileSpecDiff:
    def test_ignores_the_live_spec_and_renders_from_inputs(self):
        """The reconciler owns the whole CR: base fields render from cfg and
        combinations from the database, so a stale CR is overwritten rather
        than merged. The deployment default plan leads the list -- it is a
        fallback and the user combos must be able to suppress it."""
        from gpustack.gateway.rate_limit import default_plan_combination

        cfg = make_cfg(default_rpm=60, default_tpm=100_000)
        combos = [{"name": "user-1", "match": []}]

        stale = {"defaultConfig": {"rule_name": "ancient"}}
        spec = reconcile_spec_diff(stale, cfg, combos)

        assert spec.defaultConfig["rule_name"] == "gpustack-rate-limit"
        assert spec.defaultConfig["limit_combinations"] == (
            [default_plan_combination(cfg)] + combos
        )
        assert spec.priority == 600

    def test_defaults_survive_a_pass_without_database_combinations(self):
        """The static pass bootstraps the CR with the default plan, but the
        reconciler owns it from its first pass on -- a full write that dropped
        the defaults would silence the deployment-wide limits until the next
        server restart."""
        cfg = make_cfg(default_rpm=60)
        spec = reconcile_spec_diff(None, cfg, [])
        (combo,) = spec.defaultConfig["limit_combinations"]
        assert combo["name"] == "default-plan"
        assert combo["is_fallback"] is True

    def test_no_combinations_leaves_the_base_without_the_key(self):
        """Every default off (``default_concurrency=0``) and an empty
        database: the plugin stays a passthrough instead of enforcing an
        empty plan. With the concurrency default of 1 the default plan is
        never empty."""
        spec = reconcile_spec_diff(None, make_cfg(default_concurrency=0), [])
        assert "limit_combinations" not in spec.defaultConfig


class TestReconcilePass:
    """One pass end to end, with the k8s write stubbed at the module
    boundary. The row-to-combination mapping is covered by the builder tests
    above; this checks the pass wires it into the right CR."""

    @pytest.mark.asyncio
    async def test_a_pass_writes_the_reconciler_spec_to_the_right_cr(self, monkeypatch):
        from gpustack.server.gateway_rate_limit_reconciler import (
            GatewayRateLimitReconciler,
        )

        applied = []

        async def _ensure(**kwargs):
            applied.append(kwargs)

        async def _build(self):
            return [{"name": "user-7", "match": []}]

        monkeypatch.setattr(
            "gpustack.server.gateway_rate_limit_reconciler.ensure_wasm_plugin",
            _ensure,
        )
        monkeypatch.setattr(
            "gpustack.server.gateway_rate_limit_reconciler."
            "GatewayRateLimitReconciler._build_combinations",
            _build,
        )

        reconciler = GatewayRateLimitReconciler.__new__(GatewayRateLimitReconciler)
        reconciler._config = make_cfg(default_rpm=60)
        reconciler._config.gateway_namespace = "higress-system"
        reconciler._budget = 100_000
        reconciler._extensions_api = object()

        await reconciler.reconcile()

        assert len(applied) == 1
        call = applied[0]
        assert call["name"] == "gpustack-rate-limit"
        assert call["namespace"] == "higress-system"
        spec = call["spec_diff"](None)
        combos = spec.defaultConfig["limit_combinations"]
        assert [combo["name"] for combo in combos] == ["default-plan", "user-7"]
