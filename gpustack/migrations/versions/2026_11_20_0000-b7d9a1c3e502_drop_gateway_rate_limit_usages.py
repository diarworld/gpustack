"""Drop the gateway rate-limit usage snapshot table.

The flusher that wrote ``gateway_rate_limit_usages`` is gone. The table
was write-only -- nothing in the codebase ever read it -- and its
rolling-window snapshots created a fresh row per flush tick (the window
start moves with ``now``), which does not scale past a handful of users.
The enforcement state lives only in the plugin's Redis counters now;
after a counter loss the server rebuilds them from
``model_usage_details`` (see
``gpustack.server.rate_limit_usage_rebuilder``).

Revision ID: b7d9a1c3e502
Revises: a3c5e7f9b104
Create Date: 2026-11-20 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import table_exists
from gpustack.schemas.common import UTCDateTime


revision: str = 'b7d9a1c3e502'
down_revision: Union[str, None] = 'a3c5e7f9b104'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if table_exists('gateway_rate_limit_usages'):
        op.drop_table('gateway_rate_limit_usages')


def downgrade() -> None:
    # Recreate the snapshot table so a pre-rebuild flusher still boots; its
    # rows are gone and it will simply repopulate from Redis.
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
