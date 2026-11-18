"""Spec builders for the gateway's ``gpustack-rate-limit`` plugin.

Shared by the static publication pass (``gateway.__init__``) and the server
side reconciler, so the deployment defaults and the database-driven
overrides render through one code path. The plugin's own documentation is the
source of truth for the configuration shape; this module only assembles it.

Combination semantics worth restating, because the reconciler leans on both:

* Combos in the non-fallback group AND together -- a request bound by a
  per-key combo and a per-user budget combo must satisfy both.
* Any non-fallback match suppresses every fallback combo for that request --
  which is what lets a per-key override replace the deployment default
  instead of being clamped back down by it.
"""

from typing import Any, Dict, List, Optional

from gpustack.config.config import Config
from gpustack.gateway.client import WasmPluginSpec
from gpustack.gateway.plugins import plugin_spec_overrides

rate_limit_plugin_name = "gpustack-rate-limit"

# Matches both consumer shapes ext-auth can produce -- the API-key form
# ``<access_key>.gpustack-<user-id>`` and the UI form ``gpustack-<user-id>``
# -- and keys the rate-limit bucket on the captured user segment, so one
# user's API traffic and dashboard traffic draw on the same allowance. The
# capture-group form is the plugin's documented way to do this; a plain
# ``regexp:`` match would key on the whole consumer string and split the
# buckets.
CONSUMER_USER_CAPTURE = r"regexp_capture:^(?:[^.]+\.)?(gpustack-.+)$"

# Rolling window of a weekly budget. The plugin has no calendar week, so the
# weekly allowance is a sliding 7-day window -- always on, never aligned to
# Monday.
WEEK_IN_SECONDS = 7 * 24 * 3600


def consumer_user_capture(user_id: int) -> str:
    """A capture match that buckets one user's two consumer shapes together."""
    return rf"regexp_capture:^(?:[^.]+\.)?(gpustack-{user_id})$"


def default_plan_combination(cfg: Config) -> Optional[Dict[str, Any]]:
    """The deployment-wide defaults as one fallback combination.

    ``None`` when no default is configured: the plugin stays a passthrough
    until the reconciler has database-driven combinations to add.
    """
    rlc = cfg.gateway_rate_limit
    query_limits = {"per_minute": rlc.default_rpm} if rlc.default_rpm else None
    token_limits = {"per_minute": rlc.default_tpm} if rlc.default_tpm else None
    token_quota: Dict[str, int] = {}
    if rlc.default_daily_tokens:
        token_quota["each_day"] = rlc.default_daily_tokens
    if rlc.default_monthly_tokens:
        token_quota["each_month"] = rlc.default_monthly_tokens
    if not (query_limits or token_limits or token_quota):
        return None

    combo: Dict[str, Any] = {
        "name": "default-plan",
        "is_fallback": True,
        "match": [{"source": "consumer", "value": CONSUMER_USER_CAPTURE}],
    }
    if query_limits:
        combo["query_limits"] = query_limits
    if token_limits:
        combo["token_limits"] = token_limits
    if token_quota:
        combo["token_quota"] = token_quota
    return combo


def rate_limit_base_config(cfg: Config) -> Dict[str, Any]:
    """The ``defaultConfig`` fields that do not depend on the database."""
    rlc = cfg.gateway_rate_limit
    config: Dict[str, Any] = {
        "rule_name": rate_limit_plugin_name,
        "timezone": rlc.timezone,
        "rejected_code": rlc.rejected_code,
        "rejected_msg": rlc.rejected_msg,
        # The rejected response carries the triggering Redis key, which is
        # how an operator maps a 429 back to the user and window that
        # produced it.
        "show_limit_quota_header": True,
        **rlc.plugin_config,
    }
    if rlc.redis is not None:
        config["redis"] = {
            "service_name": rlc.redis.service_name,
            "service_port": rlc.redis.service_port,
            "username": rlc.redis.username,
            "password": rlc.redis.password,
            "timeout": rlc.redis.timeout,
            "database": rlc.redis.database,
        }
    return config


def rate_limit_spec(cfg: Config, combinations: List[Dict[str, Any]]) -> WasmPluginSpec:
    """The full expected spec for one set of combinations.

    Priority 600 is the slot the plugin chain reserves for rate limiting:
    after ext-auth (360) injects the consumer identity in the request path,
    and after ai-statistics (900) has written ``ai_log`` by the time this
    plugin's response-phase hook reads the token counts from it.
    """
    default_config = rate_limit_base_config(cfg)
    if combinations:
        default_config["limit_combinations"] = list(combinations)
    return WasmPluginSpec(
        defaultConfig=default_config,
        defaultConfigDisable=False,
        failStrategy="FAIL_OPEN",
        imagePullPolicy="UNSPECIFIED_POLICY",
        matchRules=[],
        phase="UNSPECIFIED_PHASE",
        priority=600,
        **plugin_spec_overrides(rate_limit_plugin_name, cfg=cfg),
    )
