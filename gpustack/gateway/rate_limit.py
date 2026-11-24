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

from gpustack.api.auth import GATEWAY_AUTH_TOKEN_HEADER
from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.gateway.client import McpBridgeRegistry, WasmPluginSpec
from gpustack.gateway.plugins import plugin_spec_overrides
from gpustack.gateway.rate_limit_settings import FreeWindow
from gpustack.gateway.server_registry import get_gpustack_higress_registry

rate_limit_plugin_name = "gpustack-rate-limit"

# The server path rejection reports are POSTed to. The gateway addresses the
# server through the McpBridge registry (see ``server_registry``), and the
# endpoint authenticates with the same derived gateway token the token-usage
# metrics report uses.
RATE_LIMIT_REPORT_PATH = "/v2/usage/gateway-limit-events"

# The McpBridge registry that carries the rate-limit redis service. This
# gateway publishes no Kubernetes services on its own -- every cluster it
# routes to comes from the default McpBridge's registries -- so a redis
# block naming the raw Service FQDN would address an Envoy cluster that
# never exists (the reason the LB plugin registers its redis the same
# way). ``ensure_mcp_resources`` creates the entry; the plugin config
# names it as ``<name>.dns``, which is what maps to a live cluster.
RATE_LIMIT_REDIS_REGISTRY_NAME = "gpustack-rate-limit-redis"

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


def user_concurrency_limits(
    user: Any, rlc: GatewayRateLimitConfig
) -> Optional[Dict[str, Any]]:
    """The ``concurrency_limits`` block for one user's combo, or ``None``.

    ``user`` is a Principal row (only ``is_admin`` and
    ``model_concurrency_limits`` are read; duck-typed so tests can pass
    simple stand-ins). Admins are exempt by omission: their combo is a
    non-fallback match, so leaving the block off suppresses the fallback
    plan's cap for everything they run. Everyone else gets the deployment
    default restated as the block's ``default`` -- the user combo
    suppresses the fallback, so inheriting means restating, exactly like
    the rate fields -- plus the user's per-model overrides. A stored 0 is
    an explicit unlimited for that model; ``default`` is only emitted when
    the deployment cap is on, so ``{"models": {...}}`` alone caps just the
    named models.
    """
    if getattr(user, "is_admin", False):
        return None
    overrides = getattr(user, "model_concurrency_limits", None) or {}
    default = rlc.default_concurrency
    if not default and not overrides:
        return None
    block: Dict[str, Any] = {}
    if default:
        block["default"] = default
    if overrides:
        block["models"] = dict(overrides)
    return block


def default_plan_combination(rlc: GatewayRateLimitConfig) -> Optional[Dict[str, Any]]:
    """The deployment-wide defaults as one fallback combination.

    ``None`` when no default is configured: the plugin stays a passthrough
    until the reconciler has database-driven combinations to add.

    Takes the effective config (the settings row already layered over the
    config file), not the raw ``Config``: the UI edits the row, and the
    fallback plan must always publish what admins currently run under.
    """
    query_limits = {"per_minute": rlc.default_rpm} if rlc.default_rpm else None
    token_limits = {"per_minute": rlc.default_tpm} if rlc.default_tpm else None
    token_quota: Dict[str, int] = {}
    if rlc.default_monthly_tokens:
        token_quota["each_month"] = rlc.default_monthly_tokens
    concurrency_limits = (
        {"default": rlc.default_concurrency} if rlc.default_concurrency else None
    )
    if not (query_limits or token_limits or token_quota or concurrency_limits):
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
    if concurrency_limits:
        combo["concurrency_limits"] = concurrency_limits
    return combo


def _redis_dns_domain(service_name: str, gateway_namespace: Optional[str]) -> str:
    """The registry domain for the redis service: a bare Service name is
    expanded to ``<name>.<namespace>.svc`` -- a single-label host does not
    resolve in Envoy's STRICT_DNS even though an in-pod client connects
    fine, and the pod's resolv.conf search list completes the cluster
    domain. Dotted names (FQDNs, the ``.dns`` registry form) and IPs pass
    through untouched."""
    if "." in service_name or not gateway_namespace:
        return service_name
    return f"{service_name}.{gateway_namespace}.svc"


def rate_limit_redis_registry(
    cfg: Config, gateway_namespace: Optional[str]
) -> Optional[McpBridgeRegistry]:
    """The McpBridge registry entry that carries the rate-limit redis
    service, or None when the feature runs without redis (disabled or
    local mode): then no cluster is needed and nothing is registered."""
    rlc = cfg.gateway_rate_limit
    if not rlc.enabled or rlc.redis is None:
        return None
    return McpBridgeRegistry(
        name=RATE_LIMIT_REDIS_REGISTRY_NAME,
        type="dns",
        domain=_redis_dns_domain(rlc.redis.service_name, gateway_namespace),
        port=rlc.redis.service_port,
        protocol="tcp",
    )


def rate_limit_base_config(
    cfg: Config, free: Optional[FreeWindow] = None
) -> Dict[str, Any]:
    """The ``defaultConfig`` fields that do not depend on the database.

    The redis block names the McpBridge registry, not the raw Service
    FQDN -- the registry is what turns the service into an Envoy cluster
    the plugin's redis client can address. ``free`` rides the base when
    the deployment carries a free-window schedule: it applies to every
    combination, database-driven or fallback.
    """
    rlc = cfg.gateway_rate_limit
    # rejected_code / rejected_msg stay absent unless the operator set them:
    # the plugin's own rejection texts name the limit axis that tripped, and
    # an explicit value here would override them.
    config: Dict[str, Any] = {
        "rule_name": rate_limit_plugin_name,
        "timezone": rlc.timezone,
        # The rejected response carries the triggering Redis key, which is
        # how an operator maps a 429 back to the user and window that
        # produced it.
        "show_limit_quota_header": True,
    }
    if free is not None and free.enabled:
        # A disabled schedule stays absent rather than sending
        # ``{"enabled": false}``: to the plugin the two are equivalent, and
        # the absent key keeps the CR free of dead config.
        config["free_windows"] = free.plugin_config()
    if rlc.rejected_code is not None:
        config["rejected_code"] = rlc.rejected_code
    if rlc.rejected_msg:
        config["rejected_msg"] = rlc.rejected_msg
    if rlc.report_events:
        # Rejection reports ride the same channel as token-usage metrics: a
        # POST to the server registry path, authenticated by the derived
        # gateway token. The server folds bursts, so the plugin reports one
        # event per rejected request. Placed before the operator pass-through
        # so ``plugin_config`` can override either block.
        registry = get_gpustack_higress_registry(cfg=cfg)
        config["endpoint"] = {
            "path": RATE_LIMIT_REPORT_PATH,
            "service_name": registry.get_service_name(),
            "service_port": registry.port,
        }
        config["header_add"] = {
            GATEWAY_AUTH_TOKEN_HEADER: cfg.get_derived_gateway_token(),
        }
    # Operator pass-through fields apply last: they override the base.
    config.update(rlc.plugin_config)
    if rlc.redis is not None:
        registry = rate_limit_redis_registry(cfg, cfg.gateway_namespace)
        config["redis"] = {
            "service_name": registry.get_service_name(),
            "service_port": registry.port,
            "username": rlc.redis.username,
            "password": rlc.redis.password,
            "timeout": rlc.redis.timeout,
            "database": rlc.redis.database,
        }
    return config


def rate_limit_spec(
    cfg: Config,
    combinations: List[Dict[str, Any]],
    free: Optional[FreeWindow] = None,
) -> WasmPluginSpec:
    """The full expected spec for one set of combinations.

    Priority 600 is the slot the plugin chain reserves for rate limiting:
    after ext-auth (360) injects the consumer identity in the request path,
    and after ai-statistics (900) has written ``ai_log`` by the time this
    plugin's response-phase hook reads the token counts from it.
    """
    default_config = rate_limit_base_config(cfg, free=free)
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
