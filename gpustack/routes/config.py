import logging
from fastapi import APIRouter, Request
from typing import Any, Dict

from gpustack.api.exceptions import InvalidException
from gpustack.config.config import Config, set_global_config
from gpustack.utils.config import (
    WHITELIST_CONFIG_FIELDS,
    READ_ONLY_CONFIG_FIELDS,
    coerce_value_by_field,
)
from gpustack.utils.rollup_tz import resolve_rollup_tz

router = APIRouter()

# The read half of the config: mounted for any authenticated user on the
# server, behind the worker token on the worker. Mutations stay on
# ``router``.
read_router = APIRouter()

logger = logging.getLogger(__name__)

# Security model for /config:
#   - GET is display-facing: the UI reads it at startup for the monitoring
#     links (grafana_url, the observability flag, the dashboard uids), so
#     the server mounts ``read_router`` for any authenticated user. No
#     secrets ride in the response -- only the whitelisted fields below.
#   - PUT mutates runtime behavior and stays admin-only: the full
#     ``router`` is mounted with Depends(get_admin_user).
#   - Worker mount (worker/worker.py) wraps both routers with
#     Depends(worker_auth) -- only callers holding the worker /
#     registration token may read or mutate that worker's runtime config.
# Authentication is intentionally enforced at the mount sites, not on the
# handlers, because the deployments use different auth mechanisms (user
# DB vs. shared token) and the worker has no access to the user DB.
# Any new mount of these routers MUST declare an appropriate auth
# dependency.


@read_router.get("/config")
async def get_config(request: Request):
    app_state = request.app.state
    cfg: Config = getattr(app_state, "server_config", None) or getattr(
        app_state, "config", None
    )
    if cfg is None:
        raise InvalidException(message="Config is not available")
    result: Dict[str, Any] = {}
    for field in READ_ONLY_CONFIG_FIELDS:
        if hasattr(cfg, field):
            result[field] = getattr(cfg, field)
    # Platform-wide business timezone (GPUSTACK_TIMEZONE) as an IANA name, so the
    # UI can render schedule/usage times in the same calendar the server uses.
    # Only emit it when it resolves to a real IANA name (ZoneInfo.key). The
    # OS-local fallback is a fixed-offset tzinfo whose str() is an ambiguous
    # abbreviation ("CST", "EDT") that Intl / dayjs.tz reject — omit it then so
    # the client falls back to the browser timezone.
    tz_key = getattr(resolve_rollup_tz(), "key", None)
    if tz_key:
        result["timezone"] = tz_key
    return result


@router.put("/config")
async def set_config(request: Request):
    app_state = request.app.state
    cfg: Config = getattr(app_state, "server_config", None) or getattr(
        app_state, "config", None
    )
    if cfg is None:
        raise InvalidException(message="Config is not available")
    data = await request.json()
    updates: Dict[str, Any] = {}
    for k, v in data.items():
        if k in WHITELIST_CONFIG_FIELDS:
            updates[k] = coerce_value_by_field(k, v)
    for k, v in updates.items():
        setattr(cfg, k, v)
    if "debug" in updates:
        logging.getLogger().setLevel(
            logging.DEBUG if bool(updates["debug"]) else logging.INFO
        )
    set_global_config(cfg)
    logger.info("Applied runtime config updates")
    return "ok"
