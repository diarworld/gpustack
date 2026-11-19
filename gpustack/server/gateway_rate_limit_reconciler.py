"""Keeps the gateway's rate-limit combinations in step with the database.

The ``gpustack-rate-limit`` plugin enforces deployment-wide defaults from
``gateway_rate_limit`` in the config file. Per-key and per-user limits are
mutable state the server owns, so this reconciler plays the same role for the
rate-limit CR that :class:`~gpustack.server.gateway_auth_reconciler.
GatewayAuthReconciler` plays for ext-auth: move the server off the request
path and onto the change-propagation path. Events trigger; every pass
rebuilds the combinations from the database in full.

How the database maps onto plugin combinations -- the plugin's group
semantics (non-fallback combos AND; any non-fallback match suppresses every
fallback) decide the layout:

* **User limit combos (non-fallback).** One per active user who owns at
  least one key, matching the user's consumer shapes via a capture that
  merges the API-key and UI forms into one shared bucket. Carries everything
  that caps the user's keys together: requests and tokens per minute (the
  user's setting or the deployment default) and the weekly / monthly token
  budgets (setting only -- the monthly falls back to the deployment
  default). Non-fallback so it ANDs with the key combos below: a user cap
  must bind every key, explicitly-limited or not.
* **Key combos (non-fallback).** One per API key, matching that key's exact
  consumer string, carrying one calendar day quota -- the key's own
  ``daily_token_limit`` or the deployment default. Unset fields inherit
  rather than omit, because the fallback default plan is suppressed for
  every request these combos match; inheriting keeps a key without an
  explicit daily limit under the deployment default instead of unbounded.
* **The default plan** from the config file, unchanged: the fallback safety
  net for requests whose combos were left out under byte pressure or whose
  consumer no longer matches a live user.
"""

import asyncio
import json
import logging
from functools import partial
from typing import Any, Dict, List, Optional

from kubernetes_asyncio import client as k8s_client
from sqlmodel import select

from gpustack import envs
from gpustack.config.config import Config
from gpustack.gateway import get_async_k8s_config
from gpustack.gateway.client.extensions_higress_io_v1_api import (
    ExtensionsHigressIoV1Api,
)
from gpustack.gateway.rate_limit import (
    WEEK_IN_SECONDS,
    consumer_user_capture,
    default_plan_combination,
    rate_limit_plugin_name,
    rate_limit_spec,
)
from gpustack.gateway.utils import ensure_wasm_plugin
from gpustack.schemas.api_keys import ApiKey
from gpustack.schemas.config import GatewayModeEnum
from gpustack.schemas.principals import Principal, PrincipalType
from gpustack.security import gateway_consumer_identity, is_legacy_token_access_key
from gpustack.server.bus import EventType
from gpustack.server.db import async_session

logger = logging.getLogger(__name__)

# Bounds on how fast a dropped event watch reconnects. Same shape as the
# ext-auth reconciler's, for the same reason.
_WATCH_RETRY_MIN_SECONDS = 1
_WATCH_RETRY_MAX_SECONDS = 30


def key_combination(key: ApiKey, cfg: Config) -> Optional[Dict[str, Any]]:
    """The per-key combo: one calendar day quota from the key's own setting
    or the deployment default, or None if it renders to nothing.

    Rate limits (requests and tokens per minute) and the weekly / monthly
    token budgets are user-level limits, carried by
    :func:`user_limits_combination` -- a combo cannot host two windows of
    the same granularity, and the user's caps must AND with every key's
    own, so they live in a separate combination that matches the same
    requests. The daily quota is the one window that is per-key by design.
    """
    rlc = cfg.gateway_rate_limit
    daily = (
        key.daily_token_limit
        if key.daily_token_limit is not None
        else rlc.default_daily_tokens
    )
    if not daily:
        return None
    return {
        "name": f"key-{key.id}",
        "match": [
            {
                "source": "consumer",
                "value": gateway_consumer_identity(key.access_key, key.user_id),
            }
        ],
        "token_quota": {"each_day": daily},
    }


def user_limits_combination(user: Principal, cfg: Config) -> Optional[Dict[str, Any]]:
    """The per-user combo: every limit that caps the user's keys together.

    Non-fallback on purpose: non-fallback combos AND, so this binds every
    request of the user alongside their keys' own day quotas. The match
    captures the user suffix, merging the API-key and UI consumer shapes
    into one shared bucket. Unset fields inherit the deployment defaults
    from ``gateway_rate_limit`` -- every active user with keys gets the
    deployment plan even when an admin has set nothing, because the key
    combos these requests also match suppress the fallback default plan.
    """
    rlc = cfg.gateway_rate_limit
    combo: Dict[str, Any] = {
        "name": f"user-{user.id}",
        "match": [{"source": "consumer", "value": consumer_user_capture(user.id)}],
    }
    rpm = user.rpm_limit if user.rpm_limit is not None else rlc.default_rpm
    if rpm:
        combo["query_limits"] = {"per_minute": rpm}
    tpm = user.tpm_limit if user.tpm_limit is not None else rlc.default_tpm
    token_limits: Dict[str, Any] = {}
    if tpm:
        token_limits["per_minute"] = tpm
    if user.weekly_token_limit:
        token_limits["per_custom"] = user.weekly_token_limit
        token_limits["custom_window_seconds"] = WEEK_IN_SECONDS
    if token_limits:
        combo["token_limits"] = token_limits
    monthly = (
        user.monthly_token_limit
        if user.monthly_token_limit is not None
        else rlc.default_monthly_tokens
    )
    if monthly:
        combo["token_quota"] = {"each_month": monthly}
    if not (
        "query_limits" in combo or "token_limits" in combo or "token_quota" in combo
    ):
        return None
    return combo


def fit_combinations(
    combinations: List[Dict[str, Any]], budget: int
) -> List[Dict[str, Any]]:
    """The combinations that fit the CR byte budget.

    The default plan is not part of the input and never competes for the
    budget: ``reconcile_spec_diff`` prepends it after this fitting, so byte
    pressure can only drop database-driven combinations. Key combos are
    served before user limit combos: a dropped key combo falls back to the
    default plan's shared buckets -- tighter than the per-key day quota, but
    still bounded -- while a dropped user combo would leave the user's
    requests unbound by their rate caps and budgets.
    """
    kept: List[Dict[str, Any]] = []
    used = 0
    for combo in sorted(
        combinations, key=lambda c: 0 if c["name"].startswith("key-") else 1
    ):
        size = len(json.dumps(combo)) + 8
        if used + size > budget:
            continue
        kept.append(combo)
        used += size
    if len(kept) < len(combinations):
        logger.warning(
            "Gateway rate limit: %d combinations left out of the CR "
            "(%d byte budget). They keep the deployment default until the "
            "budget allows them.",
            len(combinations) - len(kept),
            budget,
        )
    return kept


def reconcile_spec_diff(
    current_spec: Optional[Any], cfg: Config, combinations: List[Dict[str, Any]]
) -> Any:
    """The reconciler's diff: the full expected spec, ignoring the live one.

    There is nothing to preserve -- the base renders from ``cfg`` every pass
    and the combinations from the database, so a stale CR is overwritten, not
    merged. The deployment default plan leads the list: the static pass is
    create-only bootstrap, so this reconciler's full write is the only copy
    of the defaults the CR carries once it has run -- leaving them out would
    silence the deployment-wide limits until the next server restart. A
    missing CR is rebuilt for the same reason the ext-auth reconciler
    rebuilds its own: ``ensure_wasm_plugin`` creates from this result, and an
    absent rate-limit CR means the deployment defaults are the only thing
    standing between the users and the limits they were given.
    """
    combos = list(combinations)
    default_combo = default_plan_combination(cfg)
    if default_combo is not None:
        combos.insert(0, default_combo)
    return rate_limit_spec(cfg, combos)


class GatewayRateLimitReconciler:
    """Writes the per-key and per-user combinations into the rate-limit CR."""

    def __init__(self, cfg: Config):
        self._config = cfg
        self._disabled = (
            cfg.gateway_mode == GatewayModeEnum.disabled
            or not cfg.gateway_rate_limit.enabled
        )
        self._interval = envs.GATEWAY_RATE_LIMIT_RECONCILE_INTERVAL_SECONDS
        self._budget = envs.GATEWAY_RATE_LIMIT_MAX_CR_BYTES
        self._flush_now = asyncio.Event()
        self._extensions_api: Optional[ExtensionsHigressIoV1Api] = None

    async def start(self):
        if self._disabled:
            return
        self._extensions_api = ExtensionsHigressIoV1Api(
            k8s_client.ApiClient(configuration=get_async_k8s_config(cfg=self._config))
        )
        # Reconcile once up front: the static pass published only the config
        # defaults, so the database-driven combinations are missing until the
        # first pass lands.
        self._flush_now.set()
        await asyncio.gather(
            self._watch(ApiKey, "api_key"),
            self._watch(Principal, "principal"),
            self._flush_loop(),
        )

    async def _watch(self, resource, label: str):
        """Turn events into flush urgency -- never into incremental state.

        Every direction here is worth a pass: a deletion removes a limit, an
        update can move it either way, and the row count is small next to the
        ext-auth tables, so there is nothing to gain from a relevance filter.
        """
        urgent = {EventType.UPDATED, EventType.DELETED, EventType.UNKNOWN}
        backoff = _WATCH_RETRY_MIN_SECONDS
        while True:
            try:
                async for event in resource.subscribe(
                    source=f"gateway_rate_limit_reconciler.{label}",
                    replay_existing=False,
                ):
                    backoff = _WATCH_RETRY_MIN_SECONDS
                    if event.type not in urgent:
                        continue
                    self._flush_now.set()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(
                    f"Gateway rate limit {label} watch failed, retrying in "
                    f"{backoff}s: {e}"
                )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _WATCH_RETRY_MAX_SECONDS)
            self._flush_now.set()

    async def _flush_loop(self):
        while True:
            try:
                await asyncio.wait_for(self._flush_now.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass
            self._flush_now.clear()
            try:
                await self.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"Failed to reconcile gateway rate limit: {e}")

    async def reconcile(self):
        combinations = await self._build_combinations()
        await ensure_wasm_plugin(
            api=self._extensions_api,
            name=rate_limit_plugin_name,
            namespace=self._config.gateway_namespace,
            spec_diff=partial(
                reconcile_spec_diff,
                cfg=self._config,
                combinations=combinations,
            ),
        )

    async def _build_combinations(self) -> List[Dict[str, Any]]:
        """Every database-driven combination, within the CR byte budget."""
        async with async_session() as session:
            users = (
                await session.exec(
                    select(Principal).where(
                        Principal.kind == PrincipalType.USER,
                        Principal.is_active.is_(True),
                    )
                )
            ).all()
            keys = (await session.exec(select(ApiKey))).all()

        # A user without keys never sends inference requests; skipping their
        # combo keeps the CR tight.
        keys_by_user: Dict[int, List[ApiKey]] = {}
        for key in keys:
            keys_by_user.setdefault(key.user_id, []).append(key)

        combinations: List[Dict[str, Any]] = []
        for user in users:
            if user.id not in keys_by_user:
                continue
            combo = user_limits_combination(user, self._config)
            if combo is not None:
                combinations.append(combo)
        for key in keys:
            # Deployment tokens are not user-facing inference keys, and their
            # consumer string carries no access key -- matching on it would
            # also catch that user's UI traffic.
            if is_legacy_token_access_key(key.access_key):
                continue
            combo = key_combination(key, self._config)
            if combo is not None:
                combinations.append(combo)
        return fit_combinations(combinations, self._budget)
