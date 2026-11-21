"""IdP claims captured at login on principals.

``external_attributes`` stores the OIDC claims selected by
``external_auth_claims`` at each SSO login (job title, org division,
cost center, …), keyed by claim name and rewritten on every sign-in.
JSON so quota automation can read the payload without a join table.

Revision ID: f7a8b9c0d1e2
Revises: e8a2c4d6f913
Create Date: 2026-11-22 10:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import column_exists, table_exists


revision: str = 'f7a8b9c0d1e2'
down_revision: Union[str, None] = 'e8a2c4d6f913'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if table_exists('principals') and not column_exists(
        'principals', 'external_attributes'
    ):
        op.add_column(
            'principals',
            sa.Column('external_attributes', sa.JSON(), nullable=True),
        )


def downgrade() -> None:
    if table_exists('principals') and column_exists(
        'principals', 'external_attributes'
    ):
        op.drop_column('principals', 'external_attributes')
