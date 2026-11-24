"""Buffer, fold, and persist gateway rate-limit rejection reports.

Flow: the gateway's ``gpustack-rate-limit`` plugin POSTs one event per
rejected request to ``/v2/usage/gateway-limit-events`` (the same trust
channel as the token-usage metrics report — derived gateway token, server
addressed through the McpBridge registry). This module buffers the reports
in process and flushes every ``FLUSH_INTERVAL_SECONDS`` to ``resource_events``
rows, which is what the Usage → Resource Events UI reads.

Burst safety: a rejected request costs the gateway nothing, so a client
retry storm (or a DDoS that ext-auth authenticates) can produce rejections
far faster than anyone wants rows written. Two bound layers:

* **Fold** — each flush collapses every (user, limit axis, model) group to
  at most ``_MAX_ROWS_PER_GROUP`` rows carrying exact timestamps; the
  remainder is summed into the last kept row's message (``+N suppressed``).
* **Overflow** — once the pending buffer passes its cap, ingest for a key
  stops accumulating individual events and only bumps a per-key counter;
  the flush then writes one overflow row per key (``buffer overflow, N
  events suppressed``). Both the memory and the row rate stay bounded no
  matter what the input rate does.

A rejected-but-dropped event loses its exact timestamp but never its
attribution: the overflow path keeps the (user, axis, model) key, so the UI
still answers "who tripped what, roughly how much".
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel

from gpustack import envs
from gpustack.schemas.resource_events import (
    EVENT_TYPE_LIMIT_CONCURRENCY,
    EVENT_TYPE_LIMIT_EVENT,
    EVENT_TYPE_LIMIT_QUOTA_DAILY,
    EVENT_TYPE_LIMIT_QUOTA_MONTHLY,
    EVENT_TYPE_LIMIT_QUOTA_YEARLY,
    EVENT_TYPE_LIMIT_RATE_REQUESTS,
    EVENT_TYPE_LIMIT_RATE_TOKENS,
    RESOURCE_TYPE_LIMIT,
    ResourceEvent,
)
from gpustack.server.db import async_session
from gpustack.server.services import UserService

logger = logging.getLogger(__name__)

FLUSH_INTERVAL_SECONDS = 10

# Rows written per (user, limit axis, model) group per flush. The first rows
# keep their exact timestamps; everything beyond the cap is summed into the
# last kept row's message. Burst fidelity past this point buys nothing — the
# rejected_total Prometheus counters on the gateway pods carry the exact
# per-request totals.
_MAX_ROWS_PER_GROUP = 5

_MAX_MESSAGE_CHARS = 1024
_MAX_NAME_CHARS = 255


class GatewayLimitEvent(BaseModel):
    """One rate-limit rejection reported by the gateway plugin.

    ``consumer`` is the ``x-mse-consumer`` identity ext-auth stamped on the
    request (``gpustack-<uid>`` or ``<access_key>.gpustack-<uid>``); it is
    the only attribution the plugin sees. ``combo`` / ``kind`` / ``period``
    identify the limit combination and the axis that tripped, mirroring the
    plugin's ``rejected_total`` metric labels.
    """

    occurred_at: datetime
    consumer: Optional[str] = None
    combo: Optional[str] = None
    kind: Optional[str] = None
    period: Optional[str] = None
    model: Optional[str] = None
    route: Optional[str] = None


def parse_consumer_user_id(consumer: Optional[str]) -> Optional[int]:
    """Extract the user id from an ``x-mse-consumer`` identity.

    ext-auth produces ``gpustack-<uid>`` for dashboard traffic and
    ``<access_key>.gpustack-<uid>`` for API-key traffic; the rate-limit
    bucket capture treats both as the same user, and so does this. Anything
    else is not attributable — reported but not resolved.
    """
    if not consumer:
        return None
    _, sep, tail = consumer.rpartition("gpustack-")
    if not sep or not tail.isdigit():
        return None
    return int(tail)


# Rolling windows that name their unit in the rejection text; anything else
# falls back to the generic wording, mirroring the plugin's
# ``rejectedMessage`` behaviour for unknown windows.
_WINDOW_UNITS = {
    "1s": "second",
    "60s": "minute",
    "3600s": "hour",
    "86400s": "day",
}


def limit_event_type(kind: Optional[str], period: Optional[str]) -> str:
    """Map the plugin's (kind, period) pair onto a ``resource_events``
    event type. Unknown pairs degrade to the generic ``limit_event`` so a
    future axis is still recorded instead of dropped."""
    kind = (kind or "").strip().lower()
    period = (period or "").strip().lower()
    if kind == "query":
        return EVENT_TYPE_LIMIT_RATE_REQUESTS
    if kind == "token_rolling":
        return EVENT_TYPE_LIMIT_RATE_TOKENS
    if kind == "token_calendar":
        return {
            "each_day": EVENT_TYPE_LIMIT_QUOTA_DAILY,
            "each_month": EVENT_TYPE_LIMIT_QUOTA_MONTHLY,
            "each_year": EVENT_TYPE_LIMIT_QUOTA_YEARLY,
        }.get(period, EVENT_TYPE_LIMIT_EVENT)
    if kind == "concurrency":
        return EVENT_TYPE_LIMIT_CONCURRENCY
    return EVENT_TYPE_LIMIT_EVENT


def limit_event_message(kind: Optional[str], period: Optional[str]) -> str:
    """Human-readable limit axis for the event message column — the same
    wording the plugin puts in the 429 body, minus the reset pointers."""
    kind = (kind or "").strip().lower()
    period = (period or "").strip().lower()
    if kind == "query":
        return f"Too many requests per {_WINDOW_UNITS.get(period, 'window')}"
    if kind == "token_rolling":
        if period == "604800s":
            # The plugin has no calendar week; the weekly budget is a
            # sliding 7-day window (see rate_limit.WEEK_IN_SECONDS).
            return "Weekly token quota exceeded"
        return f"Too many tokens per {_WINDOW_UNITS.get(period, 'window')}"
    if kind == "token_calendar":
        return {
            "each_day": "Daily token quota exceeded",
            "each_month": "Monthly token quota exceeded",
            "each_year": "Yearly token quota exceeded",
        }.get(period, "Token quota exceeded")
    if kind == "concurrency":
        return "Too many concurrent requests for this model"
    return "Rate limit exceeded"


_buffers_lock = asyncio.Lock()

# Raw rejection reports awaiting the next flush, in arrival order.
_pending_events: List[GatewayLimitEvent] = []

# (user_id, event_type, model) → suppressed count. Ingest that arrives once
# the pending buffer is full stops retaining individual events (the exact
# timestamps are the first casualty of an attack) and only tracks how many
# were dropped per attribution key, so the flush can still write one
# attributable row instead of losing the burst entirely.
_overflow_counts: Dict[Tuple[Optional[int], str, str], int] = {}


async def accumulate_limit_events(events: List[GatewayLimitEvent]) -> None:
    """Buffer incoming rejection reports. Called from the report endpoint;
    never blocks the gateway and never grows past the configured cap."""
    async with _buffers_lock:
        for event in events:
            if len(_pending_events) >= envs.LIMIT_EVENTS_BUFFER_MAX_SIZE:
                key = _event_key(event)
                _overflow_counts[key] = _overflow_counts.get(key, 0) + 1
                continue
            _pending_events.append(event)


def _event_key(
    event: GatewayLimitEvent,
) -> Tuple[Optional[int], str, str]:
    return (
        parse_consumer_user_id(event.consumer),
        limit_event_type(event.kind, event.period),
        event.model or "",
    )


async def flush_limit_events() -> None:
    """Swap out the buffers and write the folded rows to ``resource_events``."""
    global _pending_events, _overflow_counts
    async with _buffers_lock:
        if not _pending_events and not _overflow_counts:
            return
        pending = _pending_events
        overflows = dict(_overflow_counts)
        _pending_events = []
        _overflow_counts = {}

    try:
        await _store_limit_events(pending, overflows)
    except Exception as e:
        logger.error(f"Error flushing limit events to DB: {e}")
        # Re-buffering restores both paths through the same clamping the
        # original ingest used, so a persistent failure degrades to the
        # overflow rows instead of growing memory without bound.
        await accumulate_limit_events(pending)
        async with _buffers_lock:
            for key, count in overflows.items():
                _overflow_counts[key] = _overflow_counts.get(key, 0) + count


async def flush_limit_events_to_db() -> None:
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        try:
            await flush_limit_events()
        except asyncio.CancelledError:
            raise
        except Exception:
            # Never let a flush error escape this loop — an unhandled
            # exception propagates through the server's asyncio.gather and
            # crashes the whole process. Log and retry on the next tick.
            logger.exception("Error in limit events flush loop")


def _naive_utc(moment: datetime) -> datetime:
    """Naive UTC, matching the ``resource_events`` timestamp convention."""
    if moment.tzinfo is None:
        return moment
    return moment.astimezone(timezone.utc).replace(tzinfo=None)


def _plan_rows(
    pending: List[GatewayLimitEvent],
    overflows: Dict[Tuple[Optional[int], str, str], int],
) -> List[Tuple[Optional[int], str, datetime, GatewayLimitEvent, int, bool]]:
    """Reduce one flush's raw reports into the rows that will be written.

    Each entry is ``(user_id, event_type, occurred_at, event, suppressed,
    is_overflow)``. Folding happens per (user, axis, model) group; overflow
    keys become one synthetic row standing in for the whole suppressed tail.
    """
    groups: Dict[Tuple[Optional[int], str, str], List[GatewayLimitEvent]] = {}
    for event in pending:
        groups.setdefault(_event_key(event), []).append(event)

    planned: List[Tuple[Optional[int], str, datetime, GatewayLimitEvent, int, bool]] = (
        []
    )
    for (uid, event_type, _model), events in groups.items():
        events.sort(key=lambda e: e.occurred_at)
        kept = events[:_MAX_ROWS_PER_GROUP]
        suppressed = len(events) - len(kept)
        for index, event in enumerate(kept):
            tail = suppressed if index == len(kept) - 1 else 0
            planned.append((uid, event_type, event.occurred_at, event, tail, False))
    for (uid, event_type, model), count in overflows.items():
        synthetic = GatewayLimitEvent(
            occurred_at=datetime.now(timezone.utc),
            model=model or None,
        )
        planned.append((uid, event_type, synthetic.occurred_at, synthetic, count, True))

    # Wall-clock order across the whole batch, so the table's occurred_at
    # ordering stays interleaved the way the requests actually happened.
    planned.sort(key=lambda row: row[2])
    return planned


def _row_message(event: GatewayLimitEvent, suppressed: int, is_overflow: bool) -> str:
    """The ``event_message`` text for one planned row."""
    if is_overflow:
        return (
            "Rate limit events buffer overflow, " f"{suppressed} events suppressed"
        )[:_MAX_MESSAGE_CHARS]
    message = limit_event_message(event.kind, event.period)
    if suppressed:
        message += f", +{suppressed} suppressed"
    if event.model:
        message += f", model={event.model}"
    return message[:_MAX_MESSAGE_CHARS]


async def _store_limit_events(
    pending: List[GatewayLimitEvent],
    overflows: Dict[Tuple[Optional[int], str, str], int],
) -> None:
    if not pending and not overflows:
        return

    planned = _plan_rows(pending, overflows)

    async with async_session() as session:
        user_service = UserService(session)
        users: Dict[int, object] = {}
        for uid, event_type, occurred_at, event, suppressed, is_overflow in planned:
            user = None
            if uid is not None:
                if uid not in users:
                    users[uid] = await user_service.get_by_id(uid)
                user = users[uid]

            # An overflow row stands in for the suppressed events alone; a
            # folded row keeps one exact event plus its folded tail.
            total_count = suppressed if is_overflow else 1 + suppressed
            creator_name = getattr(user, "name", None) if user is not None else None
            resource_name = creator_name or (event.consumer or "")

            row = ResourceEvent(
                occurred_at=_naive_utc(occurred_at),
                # Personal-domain attribution: the plugin sees only the user
                # identity, not the active Org, so the rejection is scoped to
                # the user's own principal — the same rule the usage pipeline
                # applies to no-Org traffic. Managers reading scope=all see
                # every row regardless.
                consumer_principal_id=user.id if user is not None else None,
                consumer_principal_kind="user" if user is not None else None,
                creator_id=uid,
                creator_name=creator_name,
                resource_type=RESOURCE_TYPE_LIMIT,
                resource_id=uid,
                resource_name=resource_name[:_MAX_NAME_CHARS],
                event_type=event_type,
                event_message=_row_message(event, suppressed, is_overflow),
                spec_snapshot={
                    "combo": event.combo,
                    "kind": event.kind,
                    "period": event.period,
                    "model": event.model,
                    "route": event.route,
                    "count": total_count,
                },
            )
            await ResourceEvent.create(session, row, auto_commit=False)
        await session.commit()


async def pending_limit_event_count() -> int:
    """Buffer depth, for tests and diagnostics."""
    async with _buffers_lock:
        return len(_pending_events)
