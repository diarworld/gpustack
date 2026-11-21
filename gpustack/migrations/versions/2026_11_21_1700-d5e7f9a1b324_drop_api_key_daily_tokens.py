"""Drop the per-key daily token budget.

Token limits are user-level: the per-key ``daily_token_limit`` was the one
remaining per-key quota, and the usage overview plus the API-key forms no
longer expose it. The column is dropped; users who need a daily cap keep
the deployment-wide ``gateway_rate_limit`` defaults and the user-level
fields.

Revision ID: d5e7f9a1b324
Revises: b2f4d6e8a910
Create Date: 2026-11-21 17:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import column_exists, table_exists


revision: str = 'd5e7f9a1b324'
down_revision: Union[str, None] = 'b2f4d6e8a910'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if table_exists('api_keys') and column_exists('api_keys', 'daily_token_limit'):
        op.drop_column('api_keys', 'daily_token_limit')


def downgrade() -> None:
    # The dropped values are gone; the column comes back nullable-empty.
    if table_exists('api_keys') and not column_exists('api_keys', 'daily_token_limit'):
        op.add_column(
            'api_keys',
            op.column('daily_token_limit', type_=sa.BigInteger(), nullable=True),
        )
