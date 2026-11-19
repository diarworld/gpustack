import pytest
from unittest.mock import MagicMock

from gpustack.config.config import GatewayRateLimitConfig
from gpustack.gateway.rate_limit import WEEK_IN_SECONDS
from gpustack.server.gateway_rate_limit_reconciler import (
    fit_combinations,
    key_combination,
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


def make_key(id=1, access_key="ak123", user_id=42, daily_token_limit=None):
    key = MagicMock()
    key.id = id
    key.access_key = access_key
    key.user_id = user_id
    key.daily_token_limit = daily_token_limit
    return key


def make_user(id=42, **limits):
    user = MagicMock()
    user.id = id
    user.rpm_limit = limits.get("rpm_limit")
    user.tpm_limit = limits.get("tpm_limit")
    user.weekly_token_limit = limits.get("weekly_token_limit")
    user.monthly_token_limit = limits.get("monthly_token_limit")
    return user


class TestKeyCombination:
    def test_matches_the_consumer_string_byte_for_byte(self):
        """The plugin matches the value ext-auth injects; any drift between
        the two sides sends the key's traffic to a bucket nobody configured.
        The generated form is ``<access_key>.gpustack-<user-id>``."""
        combo = key_combination(
            make_key(access_key="ab12cd34", daily_token_limit=2_000_000), make_cfg()
        )
        (match,) = combo["match"]
        assert match == {
            "source": "consumer",
            "value": "ab12cd34.gpustack-42",
        }

    def test_day_quota_inherits_the_deployment_default(self):
        """The deployment default is a fallback, and any non-fallback match
        suppresses it -- inheriting is what keeps a key without an explicit
        daily limit under the deployment default instead of unbounded."""
        combo = key_combination(make_key(), make_cfg(default_daily_tokens=500_000))
        assert combo["name"] == "key-1"
        assert combo["token_quota"] == {"each_day": 500_000}
        # The day quota is the key's only window; rate limits and budgets
        # are user-level and live on the user combo.
        assert "query_limits" not in combo
        assert "token_limits" not in combo
        assert "is_fallback" not in combo

    def test_explicit_daily_wins_over_default(self):
        combo = key_combination(
            make_key(daily_token_limit=1_000),
            make_cfg(default_daily_tokens=500_000),
        )
        assert combo["token_quota"] == {"each_day": 1_000}

    def test_no_daily_anywhere_renders_nothing(self):
        assert key_combination(make_key(), make_cfg()) is None


class TestUserLimitsCombination:
    def test_buckets_both_consumer_shapes_on_the_user_segment(self):
        import re

        combo = user_limits_combination(
            make_user(), make_cfg(default_rpm=10, default_monthly_tokens=1_000_000)
        )
        (match,) = combo["match"]
        expression = match["value"].removeprefix("regexp_capture:")
        assert re.fullmatch(expression, "ak-123.gpustack-42").group(1) == "gpustack-42"
        assert re.fullmatch(expression, "gpustack-42").group(1) == "gpustack-42"
        # A different user's traffic must not land in this bucket.
        assert re.fullmatch(expression, "gpustack-43") is None

    def test_is_non_fallback_so_it_ands_with_key_combos(self):
        """A user cap must bind every key of the account, explicitly-limited
        or not: non-fallback combos AND, fallback combos are suppressed the
        moment any key combo matches."""
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
        assert user_limits_combination(make_user(), make_cfg()) is None


class TestFitCombinations:
    def test_key_combos_are_served_before_user_budgets(self):
        key_combo = {"name": "key-1", "match": []}
        user_combo = {"name": "user-2", "is_fallback": True, "match": []}
        # A budget that fits only one of the two: the key combo must win.
        kept = fit_combinations([user_combo, key_combo], budget=json_small(key_combo))
        assert kept == [key_combo]

    def test_everything_is_kept_when_it_fits(self):
        combos = [
            {"name": "key-1", "match": [{"source": "consumer", "value": "x"}]},
            {"name": "user-2", "is_fallback": True, "match": []},
        ]
        assert fit_combinations(combos, budget=10_000) == combos

    def test_zero_budget_keeps_nothing(self):
        assert fit_combinations([{"name": "key-1", "match": []}], budget=0) == []


def json_small(obj):
    import json

    return len(json.dumps(obj)) + 8


class TestReconcileSpecDiff:
    def test_ignores_the_live_spec_and_renders_from_inputs(self):
        """The reconciler owns the whole CR: base fields render from cfg and
        combinations from the database, so a stale CR is overwritten rather
        than merged. The deployment default plan leads the list -- it is a
        fallback and the key combos must be able to suppress it."""
        from gpustack.gateway.rate_limit import default_plan_combination

        cfg = make_cfg(default_rpm=60, default_tpm=100_000)
        combos = [{"name": "key-1", "match": []}]

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
        """No defaults configured and an empty database: the plugin stays a
        passthrough instead of enforcing an empty plan."""
        spec = reconcile_spec_diff(None, make_cfg(), [])
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
            return [{"name": "key-7", "match": []}]

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
        assert [combo["name"] for combo in combos] == ["default-plan", "key-7"]
