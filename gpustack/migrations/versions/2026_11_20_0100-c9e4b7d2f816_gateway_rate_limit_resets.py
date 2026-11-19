"""Rate-limit resets recorded from the Usage page.

A reset forgives a user's weekly or monthly token budget mid-window; the
row is the audit trail and the source for the floating reset quota (two
per window per user). Reference columns are plain integers, not foreign
keys, so the history outlives the users and admins it describes.

Revision ID: c9e4b7d2f816
Revises: b7d9a1c3e502
Create Date: 2026-11-20 01:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import table_exists
from gpustack.schemas.common import UTCDateTime


revision: str = 'c9e4b7d2f816'
down_revision: Union[str, None] = 'b7d9a1c3e502'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if table_exists('gateway_rate_limit_resets'):
        return
    op.create_table(
        'gateway_rate_limit_resets',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('kind', sa.String(length=32), nullable=False),
        sa.Column('reset_at', UTCDateTime(), nullable=False),
        sa.Column('reset_by_user_id', sa.Integer(), nullable=True),
        sa.Column('description', sa.Text(), nullable=False),
        sa.Column('created_at', UTCDateTime(), nullable=False),
        sa.Column('updated_at', UTCDateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_gw_rl_resets_user_id', 'gateway_rate_limit_resets', ['user_id']
    )
    op.create_index(
        'ix_gw_rl_resets_kind', 'gateway_rate_limit_resets', ['kind']
    )
    op.create_index(
        'ix_gw_rl_resets_reset_by_user_id',
        'gateway_rate_limit_resets',
        ['reset_by_user_id'],
    )


def downgrade() -> None:
    if not table_exists('gateway_rate_limit_resets'):
        return
    op.drop_index(
        'ix_gw_rl_resets_reset_by_user_id', table_name='gateway_rate_limit_resets'
    )
    op.drop_index('ix_gw_rl_resets_kind', table_name='gateway_rate_limit_resets')
    op.drop_index('ix_gw_rl_resets_user_id', table_name='gateway_rate_limit_resets')
    op.drop_table('gateway_rate_limit_resets')
