"""Deployment-wide rate-limit defaults and the free-window schedule.

A singleton row (id=1) carries the limits admins edit from the Usage
page: the per-user unset fields inherit these instead of the static
``gateway_rate_limit`` config file (which stays as the bootstrap seed for
a fresh deployment only). The free-window columns publish the schedule
during which the gateway plugin suspends every check and the limit
counters freeze. The server seeds the row on first read, so the
migration only builds the shape.

Revision ID: e8a2c4d6f913
Revises: d5e7f9a1b324
Create Date: 2026-11-21 21:30:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import table_exists
from gpustack.schemas.common import UTCDateTime


revision: str = 'e8a2c4d6f913'
down_revision: Union[str, None] = 'd5e7f9a1b324'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if table_exists('gateway_rate_limit_settings'):
        return
    op.create_table(
        'gateway_rate_limit_settings',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        # NULL means "inherit the config-file default"; an explicit value
        # overrides it from the Usage page.
        sa.Column('default_rpm', sa.Integer(), nullable=True),
        sa.Column('default_tpm', sa.BigInteger(), nullable=True),
        sa.Column('default_monthly_tokens', sa.BigInteger(), nullable=True),
        sa.Column('default_concurrency', sa.Integer(), nullable=True),
        # The free schedule: whole free days plus a daily window that may
        # wrap past midnight. All wall-clock values are UTC -- the plugin
        # only loads that timezone (see the rate-limit-redis config note).
        sa.Column('free_enabled', sa.Boolean(), nullable=False),
        sa.Column('free_start', sa.String(length=5), nullable=False),
        sa.Column('free_end', sa.String(length=5), nullable=False),
        sa.Column('free_days', sa.JSON(), nullable=False),
        # The TimestampsMixin column set; soft-delete is unused (the row is
        # a singleton) but part of the model the ORM selects.
        sa.Column('deleted_at', UTCDateTime(), nullable=True),
        sa.Column('created_at', UTCDateTime(), nullable=False),
        sa.Column('updated_at', UTCDateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade() -> None:
    if not table_exists('gateway_rate_limit_settings'):
        return
    op.drop_table('gateway_rate_limit_settings')
