"""Record the decision-model verdict on rate-limit resets.

The Usage-page reset press is gated by the d3 decision model: the
description is classified before the counters clear, and the verdict has
to outlive the request. Three nullable columns on
``gateway_rate_limit_resets``:

- ``outcome`` — ``approved`` (counters cleared) or ``rejected`` (the model
  refused the description; audit row only, never counted against the
  floating quota). Existing rows predate the gate and were unconditional
  approvals, so the backfill writes ``approved``.
- ``work_type`` — the model's classification of the task the budget is
  spent on, on an approved press (NULL when the gate was off or
  unreachable — fail-open).
- ``rejection_reason`` — why the model refused, on a rejected press.

Plain string columns, not a SQL enum: the values come from the decision
service and the enum set may grow without a migration per dialect.

Revision ID: e9c2a6d4b7f1
Revises: f3a9b5c1d7e8
Create Date: 2026-11-25 11:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

from gpustack.migrations.utils import column_exists, table_exists


revision: str = 'e9c2a6d4b7f1'
down_revision: Union[str, None] = 'f3a9b5c1d7e8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE = 'gateway_rate_limit_resets'


def upgrade() -> None:
    if not table_exists(_TABLE):
        return
    if not column_exists(_TABLE, 'outcome'):
        # The server_default backfills every existing row with ``approved``
        # as the column lands — they predate the gate and were unconditional
        # approvals.
        op.add_column(
            _TABLE,
            sa.Column(
                'outcome', sa.String(16), nullable=False, server_default='approved'
            ),
        )
    if not column_exists(_TABLE, 'work_type'):
        op.add_column(
            _TABLE,
            sa.Column('work_type', sa.String(32), nullable=True),
        )
    if not column_exists(_TABLE, 'rejection_reason'):
        op.add_column(
            _TABLE,
            sa.Column('rejection_reason', sa.String(32), nullable=True),
        )


def downgrade() -> None:
    # The columns carry only the gate's verdicts; dropping them leaves the
    # reset history itself (who, when, which window, the description)
    # untouched.
    if not table_exists(_TABLE):
        return
    for column in ('rejection_reason', 'work_type', 'outcome'):
        if column_exists(_TABLE, column):
            op.drop_column(_TABLE, column)
