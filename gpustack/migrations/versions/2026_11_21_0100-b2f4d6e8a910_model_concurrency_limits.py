"""Per-model in-flight caps on principals.

``model_concurrency_limits`` maps a model name to the number of
simultaneous requests the account may run against it (0 = explicit
unlimited); models absent from the map inherit the deployment
``gateway_rate_limit.default_concurrency``. JSON so the gateway
reconciler can render the map straight into the plugin's combo
configuration without a join table.

Revision ID: b2f4d6e8a910
Revises: c9e4b7d2f816
Create Date: 2026-11-21 01:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import column_exists, table_exists


revision: str = 'b2f4d6e8a910'
down_revision: Union[str, None] = 'c9e4b7d2f816'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if table_exists('principals') and not column_exists(
        'principals', 'model_concurrency_limits'
    ):
        op.add_column(
            'principals',
            sa.Column('model_concurrency_limits', sa.JSON(), nullable=True),
        )


def downgrade() -> None:
    if table_exists('principals') and column_exists(
        'principals', 'model_concurrency_limits'
    ):
        op.drop_column('principals', 'model_concurrency_limits')
