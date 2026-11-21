import re

import pytest
from unittest.mock import MagicMock

from gpustack.config.config import GatewayRateLimitConfig
from gpustack.gateway.rate_limit import WEEK_IN_SECONDS
from gpustack.server.gateway_rate_limit_reconciler import (
    fit_combinations,
    key_combination,
    key_combinations_for_user,
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
    # Explicit: a MagicMock attribute would auto-create a truthy is_admin
    # and silently turn every fixture user into an exempt admin.
    user.is_admin = limits.get("is_admin", False)
    user.model_concurrency_limits = limits.get("model_concurrency_limits")
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


class TestKeyCombinationsForUser:
    def test_shared_quota_collapses_into_one_regexp_combo(self):
        """Keys of one user sharing an effective day quota -- the common
        case, all inheriting the deployment default -- collapse into one
        regexp combo covering every API-key consumer of the user. No
        capture group: the whole consumer string stays the quota bucket,
        so each key keeps its own day quota instead of the keys fusing
        into one user-wide bucket."""
        combos = key_combinations_for_user(
            42,
            [make_key(id=1), make_key(id=2, access_key="ak999")],
            make_cfg(default_daily_tokens=500_000),
        )
        (combo,) = combos
        assert combo["name"] == "keys-42"
        assert combo["token_quota"] == {"each_day": 500_000}
        (match,) = combo["match"]
        assert match["source"] == "consumer"
        expression = match["value"].removeprefix("regexp:")
        assert re.fullmatch(expression, "ak123.gpustack-42")
        assert re.fullmatch(expression, "ak999.gpustack-42")
        # The bare UI consumer has no key and drew no key combo before.
        assert re.fullmatch(expression, "gpustack-42") is None
        # Another user's keys stay out.
        assert re.fullmatch(expression, "ak123.gpustack-43") is None

    def test_explicit_uniform_quota_uses_the_explicit_value(self):
        combos = key_combinations_for_user(
            42,
            [
                make_key(id=1, daily_token_limit=1_000),
                make_key(id=2, access_key="ak999", daily_token_limit=1_000),
            ],
            make_cfg(default_daily_tokens=500_000),
        )
        (combo,) = combos
        assert combo["token_quota"] == {"each_day": 1_000}

    def test_mixed_quotas_fall_back_to_exact_per_key_combos(self):
        """RE2 has no negative lookahead, so a shared regexp would overlap
        the explicit keys' matches and AND the quotas down to the smaller
        one. A user mixing explicit per-key quotas keeps one exact combo
        per key, with disjoint matches."""
        combos = key_combinations_for_user(
            42,
            [
                make_key(id=1, daily_token_limit=1_000),
                make_key(id=2, access_key="ak999"),
            ],
            make_cfg(default_daily_tokens=500_000),
        )
        assert [combo["name"] for combo in combos] == ["key-1", "key-2"]
        assert combos[0]["match"][0]["value"] == "ak123.gpustack-42"
        assert combos[0]["token_quota"] == {"each_day": 1_000}
        assert combos[1]["match"][0]["value"] == "ak999.gpustack-42"
        assert combos[1]["token_quota"] == {"each_day": 500_000}

    def test_no_daily_anywhere_renders_nothing(self):
        assert key_combinations_for_user(42, [make_key()], make_cfg()) == []

    def test_explicit_zero_is_no_quota(self):
        """An explicit 0 disables the day quota for that key -- the same
        rendering an unset daily limit gets when no default exists."""
        assert (
            key_combinations_for_user(
                42,
                [make_key(daily_token_limit=0)],
                make_cfg(default_daily_tokens=500_000),
            )
            == []
        )

    def test_no_keys_render_nothing(self):
        assert (
            key_combinations_for_user(42, [], make_cfg(default_daily_tokens=500_000))
            == []
        )


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
    def test_user_budgets_are_served_before_key_combos(self):
        """A dropped user combo unenforces the user's rate caps, budgets
        and concurrency on every request they send; a dropped key combo
        costs one key its day quota. Byte pressure must eat the narrower
        blast radius first."""
        key_combo = {"name": "key-1", "match": []}
        user_combo = {"name": "user-2", "is_fallback": True, "match": []}
        # A budget that fits only one of the two: the user combo must win.
        kept = fit_combinations([key_combo, user_combo], budget=json_small(user_combo))
        assert kept == [user_combo]

    def test_everything_is_kept_when_it_fits(self):
        user_combo = {"name": "user-2", "is_fallback": True, "match": []}
        key_combo = {"name": "key-1", "match": [{"source": "consumer", "value": "x"}]}
        # The kept list comes back in serving order: user combos lead.
        assert fit_combinations([key_combo, user_combo], budget=10_000) == [
            user_combo,
            key_combo,
        ]

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
