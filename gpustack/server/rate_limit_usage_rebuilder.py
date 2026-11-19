"""Rebuilds the gateway's Redis rate-limit counters from usage details.

The plugin's Redis counters are the only enforcement state. When the
rate-limit Redis loses them -- a fresh instance, a flush without
persistence -- calendar quotas restart from zero and users who already
spent their day / week / month budget get it back for the rest of the
window. This task replays the consumption the server already recorded in
``model_usage_details`` into the very keys the plugin reads (byte layout
in ``gpustack.gateway.rate_limit_usage``).

Two passes alternate:

* at every server startup the long token windows are rebuilt from
  scratch, counters replaced -- the database is the billing source of
  truth, so this also corrects a counter that survived but lost history
  (a young Redis that only saw the last hours of the month);
* afterwards, a loop probes the counters with one pipelined EXISTS every
  cycle and rebuilds only the ones that went missing -- a live counter is
  never touched, so a Redis flush, failover or manual wipe heals within
  one interval without touching what the gateway is counting right now.

Not exact by design:

* minute windows are not rebuilt -- they age out in 60 seconds, long
  before a rebuild can matter;
* members are per-hour aggregates (``rebuild-...|<tokens>`` scored at
  the bucket's start), so the rolling weekly window decays in hour steps
  rather than per request;
* the plugin counts at the gateway while the database records at the
  proxy, so aborted or untracked requests can leave a rebuilt total
  slightly off the lost one.

Usage recorded before a rate-limit reset (``gateway_rate_limit_resets``)
is forgiven: rows older than the user's latest reset of a window kind are
excluded, so a rebuilt weekly or monthly counter resumes from the reset
instead of resurrecting the forgiven spend.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlmodel import select

from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.gateway.rate_limit_usage import (
    RateLimitWindowSpec,
    calendar_elapsed_seconds,
    default_plan_windows,
    key_windows,
    reader_from_config,
    user_limit_windows,
)
from gpustack.schemas.api_keys import ApiKey
from gpustack.schemas.gateway_rate_limit_resets import GatewayRateLimitReset
from gpustack.schemas.model_usage_details import ModelUsageDetails
from gpustack.schemas.principals import Principal, PrincipalType
from gpustack.security import is_legacy_token_access_key
from gpustack.server.db import async_session

logger = logging.getLogger(__name__)

# The long token window periods the rebuild covers, mapped to the reset
# kind that forgives them ('plain' = no reset kind exists for the window).
_PERIOD_KINDS = {
    "604800s": "weekly",
    "each_month": "monthly",
    "each_day": "plain",
}

# How often the counters are probed for loss. The probe itself is one
# pipelined EXISTS over the published windows; the details scan only runs
# when the probe finds something missing.
REBUILD_INTERVAL_SECONDS = 300

# Members are per-hour aggregates: users x hours per window instead of
# users x requests. Commands ride one pipeline per batch, so the batch
# size costs no extra connections (see RateLimitUsageReader.window_states).
ZADD_BATCH = 500


def _window_start_ts(
    spec: RateLimitWindowSpec, now: datetime, timezone_name: str
) -> int:
    """Epoch second the spec's window began: fixed offset for rolling
    windows, the calendar boundary in the deployment timezone otherwise."""
    now_ts = int(now.timestamp())
    if spec.window_seconds:
        return now_ts - spec.window_seconds
    return now_ts - calendar_elapsed_seconds(spec.period, now, timezone_name)


def _hours_for(
    source: Dict[Tuple, Dict[int, int]], user_id: int, api_key_id: Optional[int]
) -> Dict[int, int]:
    """Token totals per hour for one window owner.

    Key windows read their own (user, key) bucket. User-level windows
    (``api_key_id`` None) aggregate every bucket of the user -- their
    keys' traffic and keyless UI traffic alike, mirroring the combo the
    reconciler publishes.
    """
    if api_key_id is not None:
        return source.get((user_id, api_key_id), {})
    merged: Dict[int, int] = {}
    for (bucket_user, _bucket_key), per_hours in source.items():
        if bucket_user != user_id:
            continue
        for hour_ts, total in per_hours.items():
            merged[hour_ts] = merged.get(hour_ts, 0) + total
    return merged


class RateLimitUsageRebuilder:
    """Periodic replay of usage details into the plugin's counters."""

    def __init__(self, config: Config):
        self._config = config
        self._warmed = False

    async def start(self):
        if self._config.gateway_rate_limit is None:
            logger.info("Rate-limit usage rebuild idle: feature disabled.")
            return
        reader = reader_from_config(self._config)
        if reader is None:
            logger.info("Rate-limit usage rebuild idle: no Redis backend.")
            return
        while True:
            try:
                await self._rebuild(self._config.gateway_rate_limit, reader.client)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # The counters stay lost until the next cycle; live traffic
                # rebuilds them the slow way -- users re-spend into fresh
                # windows -- in the meantime.
                logger.warning(
                    f"Rate-limit usage rebuild skipped: {type(e).__name__}: {e}"
                )
            await asyncio.sleep(REBUILD_INTERVAL_SECONDS)

    async def _rebuild(self, rlc: GatewayRateLimitConfig, client):
        now = datetime.now(timezone.utc)
        replace = not self._warmed
        specs = await self._target_specs(rlc)
        if not specs:
            return
        if replace:
            # Startup warm-up: every long token window is replaced with the
            # database picture, which also corrects a counter that survived
            # but lost history. Minute windows are not in the specs at all.
            targets = [
                key
                for key, (spec, _, _) in specs.items()
                if spec.period in _PERIOD_KINDS
            ]
        else:
            targets = await self._missing_keys(client, list(specs))
            if not targets:
                return
        starts = {
            redis_key: _window_start_ts(spec, now, rlc.timezone)
            for redis_key, (spec, _, _) in specs.items()
        }
        plain, forgiven, cutoffs = await self._load_usage_buckets(
            min(starts.values()), now
        )
        await self._write(
            client, specs, starts, plain, forgiven, cutoffs, targets, replace
        )
        if replace:
            # Marked only after the pass landed: a warm-up that fails
            # (database hiccup) must retry as a warm-up on the next cycle,
            # not degrade into a missing-key probe over a stale picture.
            self._warmed = True
        logger.info(
            f"Rate-limit usage rebuild: restored {len(targets)} counters "
            f"from usage details."
        )

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
                if spec.period not in _PERIOD_KINDS:
                    # Minute windows age out before a rebuild can matter;
                    # unknown periods are not understood, so not rebuilt.
                    continue
                specs[spec.redis_key] = (spec, user.id, None)
        for key in keys:
            # Deployment tokens carry no published combo (their consumer
            # string would also catch the user's UI traffic).
            if is_legacy_token_access_key(key.access_key):
                continue
            for spec in key_windows(rlc, key):
                if spec.period not in _PERIOD_KINDS:
                    continue
                specs[spec.redis_key] = (spec, key.user_id, key.id)
        return specs

    async def _load_usage_buckets(
        self, min_start_ts: int, now: datetime
    ) -> Tuple[Dict, Dict, Dict]:
        """Per (user_id, api_key_id, hour) token sums from the details table.

        Returns the row maps and the cutoffs behind them: ``plain`` holds
        every row for windows without a reset kind (the day quotas);
        ``forgiven`` holds only rows recorded after the user's latest
        reset of the kind -- a reset forgives the spend before it, and a
        rebuilt weekly / monthly counter must resume from the reset rather
        than resurrect it. ``cutoffs`` tells which (user, kind) pairs the
        ``forgiven`` map governs; the rest read ``plain``.

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
            resets = (await session.exec(select(GatewayRateLimitReset))).all()
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

        # Latest reset per (user, kind): everything strictly before it was
        # forgiven at the press. reset_at itself is the forgiveness instant,
        # so usage at or after it counts again.
        cutoffs: Dict[Tuple[int, str], datetime] = {}
        for reset in resets:
            pair = (reset.user_id, reset.kind)
            if pair not in cutoffs or reset.reset_at > cutoffs[pair]:
                cutoffs[pair] = reset.reset_at

        plain: Dict[Tuple[int, Optional[int]], Dict[int, int]] = {}
        forgiven: Dict[Tuple[str, int, Optional[int]], Dict[int, int]] = {}
        for user_id, api_key_id, ts, prompt, completion in rows:
            total = (prompt or 0) + (completion or 0)
            if not user_id or total <= 0 or ts is None:
                continue
            if ts.tzinfo is None:
                # Naive stamps are UTC, matching UTCDateTime storage.
                ts = ts.replace(tzinfo=timezone.utc)
            hour_ts = int(ts.replace(minute=0, second=0, microsecond=0).timestamp())
            per_hours = plain.setdefault((user_id, api_key_id), {})
            per_hours[hour_ts] = per_hours.get(hour_ts, 0) + total
            for kind in ("weekly", "monthly"):
                cutoff = cutoffs.get((user_id, kind))
                if cutoff is not None and ts >= cutoff:
                    per_kind = forgiven.setdefault((kind, user_id, api_key_id), {})
                    per_kind[hour_ts] = per_kind.get(hour_ts, 0) + total
        return plain, forgiven, cutoffs

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

    async def _write(
        self, client, specs, starts, plain, forgiven, cutoffs, targets, replace: bool
    ):
        """ZADD the per-hour members of every target counter.

        ``replace`` (the startup warm-up) DELs the counter first: the
        database picture replaces whatever survived. Otherwise the EXISTS
        gate kept live counters out of the batch, so live traffic racing
        the rebuild can only add. ZADD merges into a member set either
        way, never overwriting a live member.
        """
        commands: List[Tuple[str, Optional[Dict[str, int]]]] = []
        for redis_key in targets:
            spec, user_id, api_key_id = specs[redis_key]
            start_ts = starts[redis_key]
            kind = _PERIOD_KINDS[spec.period]
            if kind == "plain" or (user_id, kind) not in cutoffs:
                # No reset kind for the window, or no reset ever recorded:
                # every row in the window counts.
                per_hours = _hours_for(plain, user_id, api_key_id)
            elif api_key_id is None:
                # Reset recorded: only the spend at or after it counts, so
                # the user-level window merges the post-reset buckets.
                merged: Dict[int, int] = {}
                for (bucket_kind, bucket_user, _bucket_key), hours in forgiven.items():
                    if bucket_kind != kind or bucket_user != user_id:
                        continue
                    for hour_ts, total in hours.items():
                        merged[hour_ts] = merged.get(hour_ts, 0) + total
                per_hours = merged
            else:
                # Key window with a reset: its own post-reset bucket. No
                # entry means nothing was consumed since the reset.
                per_hours = forgiven.get((kind, user_id, api_key_id), {})
            members: Dict[str, int] = {}
            for hour_ts, total in per_hours.items():
                if hour_ts < start_ts:
                    continue
                # The trailing |<total> is the weight the reader (and
                # the plugin) parse; scored one second into the bucket
                # because the read's lower bound is exclusive at the
                # window start. The name carries the bucket identity so
                # two sources feeding the same hour stay distinct
                # members instead of overwriting each other.
                member = f"rebuild-{user_id}-{api_key_id or 0}-{hour_ts}|{total}"
                members[member] = hour_ts + 1
            if not members and not replace:
                # Nothing to restore for a missing counter.
                continue
            # None members = DEL only: the stale counter must not survive
            # the warm-up even when the database has nothing in the window.
            commands.append((redis_key, members or None))

        for i in range(0, len(commands), ZADD_BATCH):
            pipe = client.pipeline(transaction=False)
            for redis_key, members in commands[i : i + ZADD_BATCH]:
                if members is None:
                    pipe.delete(redis_key)
                else:
                    pipe.zadd(redis_key, members)
            await pipe.execute(raise_on_error=True)
