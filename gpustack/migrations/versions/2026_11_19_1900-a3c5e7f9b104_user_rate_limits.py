"""User rate limits move from API keys to users.

Requests and tokens per minute are user-level caps that bind every key of
the account together, so the columns live on principals next to the token
budgets; the per-key rpm/tpm columns on api_keys are gone (the per-key
limit that remains is the daily token budget).

Revision ID: a3c5e7f9b104
Revises: f1a9c3e7b5d2
Create Date: 2026-11-19 19:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import column_exists, table_exists


revision: str = 'a3c5e7f9b104'
down_revision: Union[str, None] = 'f1a9c3e7b5d2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if table_exists('principals'):
        for column in ('rpm_limit', 'tpm_limit'):
            if not column_exists('principals', column):
                # BigInteger: token budgets are routine in the billions and
                # int32 tops out at ~2.1e9; keep the family uniform.
                op.add_column(
                    'principals',
                    sa.Column(column, sa.BigInteger(), nullable=True),
                )
    if table_exists('api_keys'):
        for column in ('rpm_limit', 'tpm_limit'):
            if column_exists('api_keys', column):
                op.drop_column('api_keys', column)


def downgrade() -> None:
    if table_exists('api_keys'):
        for column in ('rpm_limit', 'tpm_limit'):
            if not column_exists('api_keys', column):
                op.add_column(
                    'api_keys',
                    sa.Column(column, sa.BigInteger(), nullable=True),
                )
    if table_exists('principals'):
        for column in ('rpm_limit', 'tpm_limit'):
            if column_exists('principals', column):
                op.drop_column('principals', column)
