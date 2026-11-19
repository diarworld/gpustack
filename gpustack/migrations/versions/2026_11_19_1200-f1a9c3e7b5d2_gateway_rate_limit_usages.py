"""Gateway rate-limit usage snapshot table.

``gateway_rate_limit_usages``: the last observed usage total per rate-limit
window, upserted per (combo_name, kind, period, window_start, user_id,
api_key_id) by the gateway rate-limit flusher. user_id / api_key_id default
to 0 rather than NULL for "not attributable" — both are part of the unique
upsert key, and MySQL unique indexes treat NULLs as distinct, which would
break the upsert.

Revision ID: f1a9c3e7b5d2
Revises: b8f4d6e2a703
Create Date: 2026-11-19 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import table_exists
from gpustack.schemas.common import UTCDateTime


revision: str = 'f1a9c3e7b5d2'
down_revision: Union[str, None] = 'b8f4d6e2a703'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if table_exists('gateway_rate_limit_usages'):
        return
    op.create_table(
        'gateway_rate_limit_usages',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        # 0 = not attributable to a specific user / API key (the default-plan
        # combo aggregates across consumers). 0 rather than NULL: part of the
        # unique upsert key, and MySQL unique indexes treat NULLs as distinct.
        sa.Column('user_id', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('api_key_id', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('combo_name', sa.String(length=255), nullable=False),
        sa.Column('kind', sa.String(length=32), nullable=False),
        sa.Column('period', sa.String(length=32), nullable=False),
        sa.Column('window_start', UTCDateTime(), nullable=False),
        sa.Column('window_seconds', sa.Integer(), nullable=True),
        sa.Column('usage', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('quota', sa.BigInteger(), nullable=True),
        sa.Column('created_at', UTCDateTime(), nullable=False),
        sa.Column('updated_at', UTCDateTime(), nullable=False),
        sa.Column('deleted_at', UTCDateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'combo_name',
            'kind',
            'period',
            'window_start',
            'user_id',
            'api_key_id',
            name='uq_gw_rl_usage_window',
        ),
    )


def downgrade() -> None:
    if table_exists('gateway_rate_limit_usages'):
        op.drop_table('gateway_rate_limit_usages')
