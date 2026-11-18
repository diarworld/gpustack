"""Rate limit and token budget columns.

Per-key request/token rate limits on api_keys and per-user token budgets on
principals, consumed by the gateway rate-limit reconciler.

Revision ID: b8f4d6e2a703
Revises: c8d3e6f1a902
Create Date: 2026-11-18 10:30:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import column_exists, table_exists


revision: str = 'b8f4d6e2a703'
down_revision: Union[str, None] = 'c8d3e6f1a902'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# BigInteger throughout: token budgets are routine in the billions and int32
# tops out at ~2.1e9.
_RATE_LIMIT_COLUMNS = {
    'api_keys': (
        'rpm_limit',
        'tpm_limit',
        'daily_token_limit',
    ),
    'principals': (
        'weekly_token_limit',
        'monthly_token_limit',
    ),
}


def upgrade() -> None:
    for table, columns in _RATE_LIMIT_COLUMNS.items():
        if not table_exists(table):
            continue
        for column in columns:
            if not column_exists(table, column):
                op.add_column(
                    table,
                    sa.Column(column, sa.BigInteger(), nullable=True),
                )


def downgrade() -> None:
    for table, columns in _RATE_LIMIT_COLUMNS.items():
        if not table_exists(table):
            continue
        for column in columns:
            if column_exists(table, column):
                op.drop_column(table, column)
