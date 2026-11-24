"""Record the rejection count on rate-limit event rows.

A ``resource_events`` row for ``resource_type='limit'`` can stand for more
than one rejection: the collector folds a burst into at most five rows per
flush (the tail counted on the last kept row) and writes one synthetic row
when the ingest buffer overflows. The authoritative count already lives in
``spec_snapshot.count``; extracting it back out of JSON for SQL aggregation
is dialect-specific across the four supported stores, so the count gets a
plain nullable integer column on both the hot and the archive table instead.
Rows written before the column keep ``NULL`` (= 1); this revision parses the
snapshot for the limit rows that already exist.

The same pass re-types the weekly rows: the sliding 7-day budget used to
share ``limit_rate_tokens`` with the per-minute TPM axis and is now
``limit_quota_weekly``.

Revision ID: f3a9b5c1d7e8
Revises: b7d3e9f1a2c4
Create Date: 2026-11-24 13:00:00.000000
"""

import json
import re
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from gpustack.migrations.utils import column_exists, table_exists


revision: str = 'f3a9b5c1d7e8'
down_revision: Union[str, None] = 'b7d3e9f1a2c4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_WEEKLY_PREFIX = 'Weekly token quota exceeded'
_FOLDED_TAIL_RE = re.compile(r', \+(\d+) suppressed$')
_OVERFLOW_RE = re.compile(r'buffer overflow, (\d+) events suppressed')


def _count_from(snapshot: object, message: object) -> int:
    """The rejection count one row stands for, from its snapshot (the
    authoritative value the collector wrote) or its message text."""
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except (TypeError, ValueError):
            snapshot = None
    if isinstance(snapshot, dict):
        raw = snapshot.get('count')
        if isinstance(raw, int) and raw >= 1:
            return raw
    if isinstance(message, str):
        overflow = _OVERFLOW_RE.search(message)
        if overflow:
            return int(overflow.group(1))
        folded = _FOLDED_TAIL_RE.search(message)
        if folded:
            return 1 + int(folded.group(1))
    return 1


def upgrade() -> None:
    for table in ('resource_events', 'resource_events_archive'):
        if not table_exists(table):
            continue
        if not column_exists(table, 'event_count'):
            op.add_column(
                table,
                sa.Column('event_count', sa.Integer(), nullable=True),
            )

    if not table_exists('resource_events') or not column_exists(
        'resource_events', 'event_count'
    ):
        return

    conn = op.get_bind()
    rows = conn.execute(
        text(
            "SELECT id, event_type, event_message, spec_snapshot "
            "FROM resource_events "
            "WHERE resource_type = 'limit' AND event_count IS NULL"
        )
    ).fetchall()
    updates = []
    for row_id, event_type, message, snapshot in rows:
        count = _count_from(snapshot, message)
        new_type = event_type
        if (
            event_type == 'limit_rate_tokens'
            and isinstance(message, str)
            and message.startswith(_WEEKLY_PREFIX)
        ):
            new_type = 'limit_quota_weekly'
        updates.append({'id': row_id, 'c': count, 't': new_type})
    if updates:
        conn.execute(
            text(
                "UPDATE resource_events SET event_count = :c, "
                "event_type = :t WHERE id = :id"
            ),
            updates,
        )


def downgrade() -> None:
    # The counts remain derivable from spec_snapshot, so dropping the column
    # loses nothing that is not re-computable; the re-typed weekly rows stay
    # under their new event type.
    for table in ('resource_events_archive', 'resource_events'):
        if table_exists(table) and column_exists(table, 'event_count'):
            op.drop_column(table, 'event_count')
