"""Persists long-window rate-limit usage snapshots from Redis into the DB.

The ``gpustack-rate-limit`` plugin keeps every window's live total in Redis
ZSETs that age out by themselves; analytics needs the slower-moving picture on
disk. This flusher periodically SCANs the plugin's counters, reads each
discovered window through :func:`gpustack.gateway.rate_limit_usage.read_windows`
-- the same batch helper and fail-open contract the usage endpoint uses -- and
upserts one :class:`~gpustack.schemas.gateway_rate_limit_usages.
GatewayRateLimitUsage` row per (combo, kind, period, window_start) holding the
last observed total. The per-request consumption time series already lives in
``model_usages``; this table exists for window-level analytics.

Every cycle shares one aware ``now``: the Redis reads, the window-start math
and the ``updated_at`` stamps must all describe a single instant. Redis reads
fail open (the cycle is dropped, the DB untouched). Attribution is deliberately
tolerant -- an off-layout key or an unknown combo is skipped with a debug log,
never raised, so one malformed counter cannot wedge the loop.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import redis.asyncio as redis_asyncio
from sqlalchemy import delete
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.gateway.rate_limit_usage import (
    CALENDAR_PERIODS,
    CONSUMER_DIM_PREFIX,
    DEFAULT_PLAN_COMBO,
    ROLLING_TOKEN_PERIOD,
    RULE_NAME,
    USER_IDENTITY_PREFIX,
    RateLimitUsageReader,
    RateLimitWindowState,
    RateLimitWindowSpec,
    calendar_elapsed_seconds,
    default_plan_windows,
    expected_windows,
    read_windows,
    reader_from_config,
    user_limit_windows,
)
from gpustack.schemas.api_keys import ApiKey
from gpustack.schemas.gateway_rate_limit_usages import GatewayRateLimitUsage
from gpustack.schemas.principals import Principal
from gpustack.security import gateway_consumer_identity
from gpustack.server.db import async_session

logger = logging.getLogger(__name__)

# Only token windows are persisted, and only their long ones: per-minute
# windows (request counts and per-minute tokens alike) churn a row every
# cycle and are already covered by model_usages, while the weekly and
# calendar snapshots are the table's analytics payload. Retention bounds
# the rest.
FLUSH_KINDS = ("token_rolling", "token_calendar")
# The per-minute rolling token period, from the shared period vocabulary.
MINUTE_PERIOD = ROLLING_TOKEN_PERIOD

# Snapshots older than this are pruned in the same pass that upserts.
RETENTION_DAYS = 90

# SCAN hint per call; the loop runs until the cursor wraps to 0 regardless.
SCAN_BATCH = 500


@dataclass
class ParsedKey:
    """The identity one Redis counter key carries, structurally parsed.

    ``combo`` is the key's second segment (``default-plan`` / ``user-<id>`` /
    ``key-<id>``), ``identity`` the value of its single consumer-dim
    fragment (``CONSUMER_DIM_PREFIX`` + identity), and the (kind, period,
    window_seconds) triple the suffix's metric: rolling suffixes render as
    ``<seconds>s``, calendar ones as ``each_<unit>`` with ``window_seconds``
    unset (their length follows the calendar, not a constant).
    """

    combo: str
    kind: str
    period: str
    window_seconds: Optional[int]
    identity: str


def _rolling_seconds(period: str) -> Optional[int]:
    """Seconds in a ``<n>s`` rolling period, or None when off-format."""
    if not period.endswith("s"):
        return None
    digits = period[:-1]
    if not digits.isascii() or not digits.isdigit():
        return None
    seconds = int(digits)
    return seconds if seconds > 0 else None


def _parse_suffix(suffix: str) -> Tuple[str, str, Optional[int]]:
    """(kind, period, window_seconds) for one key suffix.

    ``q:<n>s`` is a rolling query window, ``t:<n>s`` a rolling token one and
    ``t:each_<unit>`` a calendar token quota. A (None, ...) result marks a
    suffix the plugin never writes.
    """
    prefix, _, period = suffix.partition(":")
    if prefix == "q":
        window = _rolling_seconds(period)
        return ("query", period, window) if window is not None else (None, "", None)
    if prefix == "t":
        if period in CALENDAR_PERIODS:
            return ("token_calendar", period, None)
        window = _rolling_seconds(period)
        return (
            ("token_rolling", period, window)
            if window is not None
            else (None, "", None)
        )
    return (None, "", None)


def parse_rate_limit_key(key: str) -> Optional[ParsedKey]:
    """Parse one counter key into its combo, metric and consumer identity.

    Tolerant by contract: anything off-layout -- a foreign rule name, a
    suffix the plugin never writes, a missing or duplicated consumer dim --
    parses as ``None`` so the caller can skip it with a debug log. The plugin
    composes these keys from published combinations; a layout this code does
    not recognize is a plugin this code must not guess about.

    Args:
        key: the Redis key, ``<rule>|<combo>|<dims joined by "|">|<suffix>``.

    Returns:
        ParsedKey: the parsed identity, or None when the key is off-layout.
    """
    segments = key.split("|")
    if len(segments) < 4 or segments[0] != RULE_NAME:
        return None
    dims = segments[2:-1]
    consumers = [
        dim[len(CONSUMER_DIM_PREFIX) :]
        for dim in dims
        if dim.startswith(CONSUMER_DIM_PREFIX)
    ]
    if len(consumers) != 1 or not consumers[0]:
        return None
    kind, period, window_seconds = _parse_suffix(segments[-1])
    if kind is None:
        return None
    return ParsedKey(
        combo=segments[1],
        kind=kind,
        period=period,
        window_seconds=window_seconds,
        identity=consumers[0],
    )


def _int_tail(segment: str, prefix: str) -> Optional[int]:
    """The digits after ``prefix`` in a segment, or None when absent."""
    tail = segment[len(prefix) :]
    if not tail.isascii() or not tail.isdigit():
        return None
    return int(tail)


def _user_from_identity(identity: str) -> int:
    """The ``gpustack-<id>`` user a fallback combo's identity buckets on.

    0 (unattributable) when the segment does not parse: the row still lands,
    keyed on user_id 0, rather than being silently dropped.
    """
    user_id = _int_tail(identity, USER_IDENTITY_PREFIX)
    if user_id is not None:
        return user_id
    logger.debug("Unattributable default-plan consumer: %s", identity)
    return 0


def _match_quota(specs: List[RateLimitWindowSpec], parsed: ParsedKey) -> Optional[int]:
    """The enforced limit for the parsed (kind, period) among a combo's
    expected windows; None when the window is unlimited or unrecognized."""
    for spec in specs:
        if spec.kind == parsed.kind and spec.period == parsed.period:
            return spec.limit
    return None


@dataclass
class Attribution:
    """Where one flushed window's row lands and what its quota was."""

    combo_name: str
    kind: str
    period: str
    window_seconds: Optional[int]
    user_id: int
    api_key_id: int
    quota: Optional[int]


def _window_start(item: Attribution, now: datetime, timezone_name: str) -> datetime:
    """The instant the snapshot's window began, from the cycle's shared now.

    Rolling windows slide with every read, so their start is
    ``now - window_seconds``: the row records which slice of history the
    total covers, not a stable bucket. Calendar windows start at the period
    boundary in the deployment timezone.
    """
    if item.window_seconds is not None:
        return now - timedelta(seconds=item.window_seconds)
    elapsed = calendar_elapsed_seconds(item.period, now, timezone_name)
    return now - timedelta(seconds=elapsed)


class RateLimitUsageFlusher:
    """Persists long-window rate-limit usage snapshots from Redis.

    One row per (combo, kind, period, window_start) holding the last
    observed total -- the consumption time series already lives in
    model_usages; this table exists for window-level analytics.
    """

    def __init__(self, cfg: Config):
        self._config = cfg
        rlc = cfg.gateway_rate_limit
        self._interval = rlc.usage_flush_interval_seconds if rlc else 0
        # Built on the first cycle and kept for the process lifetime: the
        # client binds its pool lazily, and rebuilding it per cycle would
        # churn connections (see reader_from_config).
        self._reader: Optional[RateLimitUsageReader] = None

    async def start(self):
        rlc = self._config.gateway_rate_limit
        if rlc is None or not rlc.enabled or rlc.redis is None:
            logger.info(
                "Rate-limit usage flusher idle: no Redis-backed "
                "gateway_rate_limit config to flush from."
            )
            return
        if self._interval <= 0:
            logger.info(
                "Rate-limit usage flusher idle: usage_flush_interval_seconds=0."
            )
            return
        while True:
            try:
                await self._flush_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # The traceback is what makes an intermittent failure (a DB
                # blip, a Redis hiccup) diagnosable after the fact.
                logger.exception(f"Failed to flush gateway rate-limit usage: {e}")
            await asyncio.sleep(self._interval)

    async def _flush_once(self):
        rlc = self._config.gateway_rate_limit
        # One aware instant shared by every read, window start and stamp in
        # this cycle: the snapshots must describe a single moment.
        now = datetime.now(timezone.utc)
        if self._reader is None:
            self._reader = reader_from_config(self._config)
        if self._reader is None:
            # start() only loops with a Redis backend; defensive no-op for
            # direct _flush_once callers.
            return
        discovered = await self._discover(self._reader.client)
        async with async_session() as session:
            if discovered:
                kept = await self._attribute(session, rlc, discovered)
                if kept:
                    specs = [
                        RateLimitWindowSpec(
                            kind=item.kind,
                            period=item.period,
                            window_seconds=item.window_seconds,
                            # The reader derives totals from the ZSET members;
                            # the limit rides along only to keep the spec
                            # complete.
                            limit=item.quota or 0,
                            redis_key=redis_key,
                        )
                        for redis_key, item in kept
                    ]
                    states, usage_error = await read_windows(
                        self._reader, specs, now=now, timezone_name=rlc.timezone
                    )
                    if usage_error is not None:
                        # Fail open: read_windows already logged the class
                        # name. The whole cycle is dropped before any DB
                        # write rather than mixing fresh and stale counters.
                        return
                    await self._upsert(
                        session,
                        [item for _, item in kept],
                        states,
                        now,
                        rlc.timezone,
                    )
            # The retention sweep runs every cycle that reaches the database:
            # a decommissioned plugin or quiet counters must not strand
            # stale snapshots behind the early returns above.
            await self._prune(session, now)
            await session.commit()

    async def _discover(
        self, client: redis_asyncio.Redis
    ) -> List[Tuple[str, ParsedKey]]:
        """Every persistable window found in one SCAN sweep.

        Explicit cursor loop (not ``scan_iter``): the plugin's key space is
        small and the exact call signature is what tests mirror.
        """
        found: List[Tuple[str, ParsedKey]] = []
        cursor = 0
        while True:
            cursor, batch = await client.scan(
                cursor, match=f"{RULE_NAME}|*", count=SCAN_BATCH
            )
            for raw in batch:
                key = (
                    raw.decode("utf-8", errors="replace")
                    if isinstance(raw, bytes)
                    else raw
                )
                parsed = parse_rate_limit_key(key)
                if parsed is None:
                    logger.debug("Skipping unparseable rate-limit key: %s", key)
                    continue
                if parsed.kind not in FLUSH_KINDS or parsed.period == MINUTE_PERIOD:
                    continue
                found.append((key, parsed))
            if cursor == 0:
                break
        return found

    async def _attribute(
        self,
        session: AsyncSession,
        rlc: GatewayRateLimitConfig,
        discovered: List[Tuple[str, ParsedKey]],
    ) -> List[Tuple[str, Attribution]]:
        """Resolve each discovered key's row identity and quota.

        Two batched reads per cycle cover every key: the ``key-<id>`` combos'
        ApiKey rows (live only -- soft-deleted keys enforce nothing) and the
        referenced users' token budgets. The combo segment is the
        authoritative attribution; the consumer dim is corroboration.
        """
        key_ids = {
            key_id
            for key_id in (_int_tail(p.combo, "key-") for _, p in discovered)
            if key_id is not None
        }
        api_keys: Dict[int, ApiKey] = {}
        if key_ids:
            rows = (
                await session.exec(
                    select(ApiKey).where(
                        ApiKey.id.in_(key_ids), ApiKey.deleted_at.is_(None)
                    )
                )
            ).all()
            api_keys = {k.id: k for k in rows}

        user_ids = {
            user_id
            for user_id in (_int_tail(p.combo, "user-") for _, p in discovered)
            if user_id is not None
        }
        user_ids.update(k.user_id for k in api_keys.values())
        users: Dict[int, Principal] = {}
        if user_ids:
            rows = (
                await session.exec(select(Principal).where(Principal.id.in_(user_ids)))
            ).all()
            users = {u.id: u for u in rows}

        attributed: List[Tuple[str, Attribution]] = []
        for redis_key, parsed in discovered:
            item = self._resolve(rlc, redis_key, parsed, api_keys, users)
            if item is not None:
                attributed.append((redis_key, item))
        return attributed

    def _resolve(
        self,
        rlc: GatewayRateLimitConfig,
        redis_key: str,
        parsed: ParsedKey,
        api_keys: Dict[int, ApiKey],
        users: Dict[int, Principal],
    ) -> Optional[Attribution]:
        """One key's row identity and quota, or None when unattributable."""
        if parsed.combo == DEFAULT_PLAN_COMBO:
            return self._resolve_default_plan(rlc, parsed)
        if parsed.combo.startswith("user-"):
            return self._resolve_user_combo(rlc, parsed, users)
        if parsed.combo.startswith("key-"):
            return self._resolve_key_combo(redis_key, parsed, api_keys, users, rlc)
        logger.debug("Unknown combo segment on %s", redis_key)
        return None

    def _resolve_default_plan(
        self, rlc: GatewayRateLimitConfig, parsed: ParsedKey
    ) -> Attribution:
        """The deployment defaults' combo: the user id comes from the
        captured ``gpustack-<id>`` dim, not from any database row. The
        fallback plan no longer governs live requests, but its Redis keys
        linger until their TTLs expire and still deserve attribution."""
        user_id = _user_from_identity(parsed.identity)
        specs = [
            spec
            for spec in default_plan_windows(rlc, user_id)
            if spec.redis_key.split("|")[1] == DEFAULT_PLAN_COMBO
        ]
        return Attribution(
            combo_name=DEFAULT_PLAN_COMBO,
            kind=parsed.kind,
            period=parsed.period,
            window_seconds=parsed.window_seconds,
            user_id=user_id,
            api_key_id=0,
            quota=_match_quota(specs, parsed),
        )

    def _resolve_user_combo(
        self,
        rlc: GatewayRateLimitConfig,
        parsed: ParsedKey,
        users: Dict[int, Principal],
    ) -> Optional[Attribution]:
        """The non-fallback combo carrying one user's shared limits."""
        user_id = _int_tail(parsed.combo, "user-")
        if user_id is None:
            logger.debug("Unparseable user combo: %s", parsed.combo)
            return None
        specs = [
            spec
            for spec in user_limit_windows(rlc, user_id, users.get(user_id))
            if spec.redis_key.split("|")[1] == parsed.combo
        ]
        return Attribution(
            combo_name=parsed.combo,
            kind=parsed.kind,
            period=parsed.period,
            window_seconds=parsed.window_seconds,
            user_id=user_id,
            api_key_id=0,
            quota=_match_quota(specs, parsed),
        )

    def _resolve_key_combo(
        self,
        redis_key: str,
        parsed: ParsedKey,
        api_keys: Dict[int, ApiKey],
        users: Dict[int, Principal],
        rlc: GatewayRateLimitConfig,
    ) -> Optional[Attribution]:
        """The non-fallback combo of one API key: its own day quota."""
        api_key_id = _int_tail(parsed.combo, "key-")
        if api_key_id is None:
            logger.debug("Unparseable key combo: %s", parsed.combo)
            return None
        key = api_keys.get(api_key_id)
        if key is None:
            # The combo segment is the authoritative attribution and the
            # snapshot must outlive the key row it names: keep the key id
            # and attribute to user 0 rather than guessing an owner.
            return Attribution(
                combo_name=parsed.combo,
                kind=parsed.kind,
                period=parsed.period,
                window_seconds=parsed.window_seconds,
                user_id=0,
                api_key_id=api_key_id,
                quota=None,
            )
        user_id = key.user_id
        identity = gateway_consumer_identity(key.access_key, user_id)
        if identity != parsed.identity:
            # Corroboration only: the combo segment still names the key, so
            # the row keeps its attribution and the drift is logged.
            logger.debug(
                "Consumer dim drift on %s: %s vs %s",
                redis_key,
                parsed.identity,
                identity,
            )
        specs = [
            spec
            for spec in expected_windows(rlc, user_id, key, users.get(user_id))
            if spec.redis_key.split("|")[1] == parsed.combo
        ]
        return Attribution(
            combo_name=parsed.combo,
            kind=parsed.kind,
            period=parsed.period,
            window_seconds=parsed.window_seconds,
            user_id=user_id,
            api_key_id=api_key_id,
            quota=_match_quota(specs, parsed),
        )

    async def _upsert(
        self,
        session: AsyncSession,
        items: List[Attribution],
        states: List[RateLimitWindowState],
        now: datetime,
        timezone_name: str,
    ):
        """UPDATE or INSERT each snapshot; one commit lands them all."""
        for item, state in zip(items, states):
            window_start = _window_start(item, now, timezone_name)
            existing = (
                await session.exec(
                    select(GatewayRateLimitUsage).where(
                        GatewayRateLimitUsage.combo_name == item.combo_name,
                        GatewayRateLimitUsage.kind == item.kind,
                        GatewayRateLimitUsage.period == item.period,
                        GatewayRateLimitUsage.window_start == window_start,
                        GatewayRateLimitUsage.user_id == item.user_id,
                        GatewayRateLimitUsage.api_key_id == item.api_key_id,
                    )
                )
            ).first()
            if existing is None:
                session.add(
                    GatewayRateLimitUsage(
                        combo_name=item.combo_name,
                        kind=item.kind,
                        period=item.period,
                        window_start=window_start,
                        window_seconds=item.window_seconds,
                        user_id=item.user_id,
                        api_key_id=item.api_key_id,
                        usage=state.used,
                        quota=item.quota,
                        updated_at=now,
                    )
                )
                continue
            existing.usage = state.used
            existing.quota = item.quota
            existing.updated_at = now

    async def _prune(self, session: AsyncSession, now: datetime):
        """Drop snapshots older than the retention window."""
        cutoff = now - timedelta(days=RETENTION_DAYS)
        await session.exec(
            delete(GatewayRateLimitUsage).where(
                GatewayRateLimitUsage.updated_at < cutoff
            )
        )
