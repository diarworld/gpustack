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

* **Key combos (non-fallback).** One per API key with an explicit
  ``rpm_limit`` / ``tpm_limit`` / ``daily_token_limit``, matching that key's
  exact consumer string. Unset fields inherit the deployment default, because
  the fallback default is suppressed for every request these combos match --
  inheriting is what keeps "I set only a TPM cap on this key" from silently
  removing its RPM guard too.
* **User budget combos (fallback).** One per user with a
  ``weekly_token_limit`` / ``monthly_token_limit``, matching that user's
  consumer shapes via a capture that merges the API-key and UI forms into one
  bucket. Fallback, so they AND with the deployment default for keys that
  carry no explicit limits, and are suppressed only where a key combo already
  matched -- and a key combo folds its owner's budgets in, so a budgeted
  user's explicitly-limited key keeps drawing on the user's allowance (as its
  own copy, not a shared bucket -- the one approximation in this design, and
  it errs in the user's favour rather than silently locking them out).
* **The default plan** from the config file, unchanged.
"""

import asyncio
import json
import logging
from functools import partial
from typing import Any, Dict, List, Optional, Tuple

from kubernetes_asyncio import client as k8s_client
from sqlalchemy import or_
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


def key_combination(
    key: ApiKey, budgets: Tuple[Optional[int], Optional[int]], cfg: Config
) -> Optional[Dict[str, Any]]:
    """The combo for one explicitly-limited API key, or None if it renders to
    nothing.

    ``budgets`` is the owner's ``(weekly, monthly)`` token allowance, folded
    in per the module docstring. Unset key fields inherit the deployment
    defaults -- see the module docstring for why inheriting is not optional.
    """
    rlc = cfg.gateway_rate_limit
    combo: Dict[str, Any] = {
        "name": f"key-{key.id}",
        "match": [
            {
                "source": "consumer",
                "value": gateway_consumer_identity(key.access_key, key.user_id),
            }
        ],
    }
    rpm = key.rpm_limit if key.rpm_limit is not None else rlc.default_rpm
    if rpm:
        combo["query_limits"] = {"per_minute": rpm}
    tpm = key.tpm_limit if key.tpm_limit is not None else rlc.default_tpm
    weekly, monthly = budgets
    token_limits: Dict[str, Any] = {}
    if tpm:
        token_limits["per_minute"] = tpm
    if weekly:
        token_limits["per_custom"] = weekly
        token_limits["custom_window_seconds"] = WEEK_IN_SECONDS
    if token_limits:
        combo["token_limits"] = token_limits
    token_quota: Dict[str, int] = {}
    daily = (
        key.daily_token_limit
        if key.daily_token_limit is not None
        else rlc.default_daily_tokens
    )
    if daily:
        token_quota["each_day"] = daily
    if monthly:
        token_quota["each_month"] = monthly
    if token_quota:
        combo["token_quota"] = token_quota
    if not (
        "query_limits" in combo or "token_limits" in combo or "token_quota" in combo
    ):
        return None
    return combo


def user_budget_combination(
    user_id: int, weekly: Optional[int], monthly: Optional[int]
) -> Optional[Dict[str, Any]]:
    """The fallback combo carrying one user's shared token budgets."""
    combo: Dict[str, Any] = {
        "name": f"user-{user_id}",
        "is_fallback": True,
        "match": [{"source": "consumer", "value": consumer_user_capture(user_id)}],
    }
    if weekly:
        combo["token_limits"] = {
            "per_custom": weekly,
            "custom_window_seconds": WEEK_IN_SECONDS,
        }
    if monthly:
        combo["token_quota"] = {"each_month": monthly}
    if "token_limits" not in combo and "token_quota" not in combo:
        return None
    return combo


def fit_combinations(
    combinations: List[Dict[str, Any]], budget: int
) -> List[Dict[str, Any]]:
    """The combinations that fit the CR byte budget.

    The default plan is not part of the input -- it is published by the
    static pass regardless -- so everything here is database-driven. Key
    combos are served before user budgets: they carry admin-set overrides,
    and a user budget dropped for want of room degrades to the deployment
    default, while a dropped key combo could land on either side of it.
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
    merged. A missing CR is rebuilt for the same reason the ext-auth
    reconciler rebuilds its own: ``ensure_wasm_plugin`` creates from this
    result, and an absent rate-limit CR means the deployment defaults are the
    only thing standing between the users and the limits they were given.
    """
    return rate_limit_spec(cfg, combinations)


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
            budgeted_users = (
                await session.exec(
                    select(Principal).where(
                        Principal.kind == PrincipalType.USER,
                        Principal.is_active.is_(True),
                        or_(
                            Principal.weekly_token_limit.is_not(None),
                            Principal.monthly_token_limit.is_not(None),
                        ),
                    )
                )
            ).all()
            limited_keys = (
                await session.exec(
                    select(ApiKey).where(
                        or_(
                            ApiKey.rpm_limit.is_not(None),
                            ApiKey.tpm_limit.is_not(None),
                            ApiKey.daily_token_limit.is_not(None),
                        ),
                    )
                )
            ).all()

        budgets = {
            user.id: (user.weekly_token_limit, user.monthly_token_limit)
            for user in budgeted_users
        }

        combinations: List[Dict[str, Any]] = []
        for key in limited_keys:
            # Deployment tokens are not user-facing inference keys, and their
            # consumer string carries no access key -- matching on it would
            # also catch that user's UI traffic.
            if is_legacy_token_access_key(key.access_key):
                continue
            combo = key_combination(
                key, budgets.get(key.user_id, (None, None)), self._config
            )
            if combo is not None:
                combinations.append(combo)
        for user_id, (weekly, monthly) in budgets.items():
            combo = user_budget_combination(user_id, weekly, monthly)
            if combo is not None:
                combinations.append(combo)
        return fit_combinations(combinations, self._budget)
