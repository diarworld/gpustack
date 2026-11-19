"""Rebuilds the gateway's Redis rate-limit counters from usage details.

The plugin's Redis counters are the only enforcement state. When the
rate-limit Redis loses them -- a fresh instance, a flush without
persistence -- calendar quotas restart from zero and users who already
spent their day / week / month budget get it back for the rest of the
window. This task replays, once per server startup, the consumption the
server already recorded in ``model_usage_details`` into the very keys the
plugin reads (byte layout in ``gpustack.gateway.rate_limit_usage``), and
only into counters that do not exist yet -- a live counter is never
touched, so against a healthy Redis this is a cheap no-op.

Not exact by design:

* minute windows are not rebuilt -- they age out in 60 seconds, long
  before a rebuild can matter;
* members are per-hour aggregates (``rebuild-<hour>|<tokens>`` scored at
  the bucket's start), so the rolling weekly window decays in hour steps
  rather than per request;
* the plugin counts at the gateway while the database records at the
  proxy, so aborted or untracked requests can leave the rebuilt total
  slightly off the lost one.

A marker key written after a successful pass (TTL past the longest
calendar window) short-circuits the details scan while the Redis keeps
its data; losing the Redis loses the marker, which re-arms the rebuild.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import func, select

from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.gateway.rate_limit import rate_limit_plugin_name
from gpustack.gateway.rate_limit_usage import (
    RateLimitWindowSpec,
    calendar_elapsed_seconds,
    default_plan_windows,
    key_windows,
    reader_from_config,
    user_limit_windows,
)
from gpustack.schemas.api_keys import ApiKey
from gpustack.schemas.model_usage_details import ModelUsageDetails
from gpustack.schemas.principals import Principal, PrincipalType
from gpustack.security import is_legacy_token_access_key
from gpustack.server.db import async_session

logger = logging.getLogger(__name__)

# A combo segment the reconciler never publishes, so the plugin can never
# write a counter under this name and collide with the marker.
REBUILD_MARKER_COMBO = "rebuild-marker"
# Outlives the longest rebuilt window (a calendar month) so a routine
# restart within the same month never rescans the details table.
MARKER_TTL_SECONDS = 35 * 24 * 3600

# Members are per-hour aggregates: users x hours per window instead of
# users x requests. Commands ride one pipeline per batch, so the batch
# size costs no extra connections (see RateLimitUsageReader.window_states).
ZADD_BATCH = 500


def _marker_key() -> str:
    return f"{rate_limit_plugin_name}|{REBUILD_MARKER_COMBO}"


def _window_start_ts(
    spec: RateLimitWindowSpec, now: datetime, timezone_name: str
) -> int:
    """Epoch second the spec's window began: fixed offset for rolling
    windows, the calendar boundary in the deployment timezone otherwise."""
    now_ts = int(now.timestamp())
    if spec.window_seconds:
        return now_ts - spec.window_seconds
    return now_ts - calendar_elapsed_seconds(spec.period, now, timezone_name)


class RateLimitUsageRebuilder:
    """One-shot startup replay of usage details into the plugin's counters."""

    def __init__(self, config: Config):
        self._config = config

    async def start(self):
        if self._config.gateway_rate_limit is None:
            logger.info("Rate-limit usage rebuild idle: feature disabled.")
            return
        reader = reader_from_config(self._config)
        if reader is None:
            logger.info("Rate-limit usage rebuild idle: no Redis backend.")
            return
        try:
            await self._rebuild(self._config.gateway_rate_limit, reader.client)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Startup resilience: the counters stay lost and live traffic
            # rebuilds them the slow way -- users re-spend into fresh
            # windows -- until the next restart retries.
            logger.warning(f"Rate-limit usage rebuild skipped: {type(e).__name__}: {e}")

    async def _rebuild(self, rlc: GatewayRateLimitConfig, client):
        now = datetime.now(timezone.utc)
        if await client.exists(_marker_key()):
            return

        specs = await self._target_specs(rlc)
        if not specs:
            return
        starts = {
            redis_key: _window_start_ts(spec, now, rlc.timezone)
            for redis_key, (spec, _, _) in specs.items()
        }
        buckets = await self._load_usage_buckets(min(starts.values()), now)
        missing = await self._missing_keys(client, list(specs))
        if missing:
            await self._write(client, specs, starts, buckets, missing)
            logger.info(
                f"Rate-limit usage rebuild: restored {len(missing)} counters "
                f"from usage details."
            )
        await client.set(_marker_key(), int(now.timestamp()), ex=MARKER_TTL_SECONDS)

    async def _target_specs(
        self, rlc: GatewayRateLimitConfig
    ) -> Dict[str, Tuple[RateLimitWindowSpec, int, Optional[int]]]:
        """Every long-window counter the plugin would enforce, mapped to its
        owner: redis_key -> (spec, user_id, api_key_id or None). User-level
        windows (the user limit combo and the default-plan fallback) bucket
        on the user across all their keys; key windows on the exact key."""
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

        keys_by_user: Dict[int, List[ApiKey]] = {}
        for key in keys:
            keys_by_user.setdefault(key.user_id, []).append(key)

        specs: Dict[str, Tuple[RateLimitWindowSpec, int, Optional[int]]] = {}
        for user in users:
            # Mirror the reconciler: a user without keys never sends
            # inference requests, so no combo is published for them.
            if user.id not in keys_by_user:
                continue
            windows = user_limit_windows(rlc, user.id, user)
            windows.extend(default_plan_windows(rlc, user.id))
            for spec in windows:
                if spec.period == "60s":
                    # Minute windows age out before a rebuild can matter.
                    continue
                specs[spec.redis_key] = (spec, user.id, None)
        for key in keys:
            # Deployment tokens carry no published combo (their consumer
            # string would also catch the user's UI traffic).
            if is_legacy_token_access_key(key.access_key):
                continue
            for spec in key_windows(rlc, key):
                specs[spec.redis_key] = (spec, key.user_id, key.id)
        return specs

    async def _load_usage_buckets(
        self, min_start_ts: int, now: datetime
    ) -> Dict[Tuple[int, Optional[int]], Dict[int, int]]:
        """Per (user_id, api_key_id, hour) token sums from the details table.

        The range rides the indexed ``date`` column; the in-window filter
        runs per row on the coalesced completion stamp, so rows whose
        ``date`` sits a timezone shift away from the window still classify
        correctly. Keyless rows (UI traffic) bucket under ``api_key_id``
        None and feed only user-level combos.
        """
        min_date = (
            datetime.fromtimestamp(min_start_ts, timezone.utc) - timedelta(days=1)
        ).date()
        async with async_session() as session:
            rows = (
                await session.exec(
                    select(
                        ModelUsageDetails.user_id,
                        ModelUsageDetails.api_key_id,
                        func.coalesce(
                            ModelUsageDetails.completed_at,
                            ModelUsageDetails.started_at,
                            ModelUsageDetails.created_at,
                        ).label("ts"),
                        ModelUsageDetails.prompt_token_count,
                        ModelUsageDetails.completion_token_count,
                    ).where(
                        ModelUsageDetails.date >= min_date,
                        ModelUsageDetails.date <= now.date(),
                    )
                )
            ).all()

        buckets: Dict[Tuple[int, Optional[int]], Dict[int, int]] = {}
        for user_id, api_key_id, ts, prompt, completion in rows:
            total = (prompt or 0) + (completion or 0)
            if not user_id or total <= 0 or ts is None:
                continue
            hour = ts.replace(minute=0, second=0, microsecond=0)
            hour_ts = int(hour.replace(tzinfo=timezone.utc).timestamp())
            per_hours = buckets.setdefault((user_id, api_key_id), {})
            per_hours[hour_ts] = per_hours.get(hour_ts, 0) + total
        return buckets

    async def _missing_keys(self, client, redis_keys: List[str]) -> List[str]:
        """Counters with no Redis key -- the only ones to rebuild. One
        failure fails the whole pass: writing half the windows would mix a
        rebuilt picture with a lost one."""
        pipe = client.pipeline(transaction=False)
        for redis_key in redis_keys:
            pipe.exists(redis_key)
        results = await pipe.execute(raise_on_error=False)
        first_error = next((r for r in results if isinstance(r, BaseException)), None)
        if first_error is not None:
            raise first_error
        return [key for key, exists in zip(redis_keys, results) if not exists]

    async def _write(self, client, specs, starts, buckets, missing):
        """ZADD the per-hour members of every missing counter. ZADD merges
        into a member set, and the EXISTS gate kept live counters out of
        the batch, so live traffic racing the rebuild can only add."""
        commands: List[Tuple[str, Dict[str, int]]] = []
        for redis_key in missing:
            spec, user_id, api_key_id = specs[redis_key]
            start_ts = starts[redis_key]
            members: Dict[str, int] = {}
            for (bucket_user, bucket_key), per_hours in buckets.items():
                if bucket_user != user_id:
                    continue
                if api_key_id is not None and bucket_key != api_key_id:
                    continue
                for hour_ts, total in per_hours.items():
                    if hour_ts < start_ts:
                        continue
                    # The trailing |<total> is the weight the reader (and
                    # the plugin) parse; scored one second into the bucket
                    # because the read's lower bound is exclusive at the
                    # window start. The name carries the bucket identity so
                    # two sources feeding the same hour stay distinct
                    # members instead of overwriting each other.
                    member = (
                        f"rebuild-{bucket_user}-{bucket_key or 0}-{hour_ts}|{total}"
                    )
                    members[member] = hour_ts + 1
            if members:
                commands.append((redis_key, members))

        for i in range(0, len(commands), ZADD_BATCH):
            pipe = client.pipeline(transaction=False)
            for redis_key, members in commands[i : i + ZADD_BATCH]:
                pipe.zadd(redis_key, members)
            await pipe.execute(raise_on_error=True)
