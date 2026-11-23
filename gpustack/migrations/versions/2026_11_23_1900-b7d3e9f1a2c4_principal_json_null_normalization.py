"""Normalize principal JSON columns holding JSON-null scalars.

SQLAlchemy's JSON type binds a plain ``None`` as the JSON scalar
``null`` — not SQL NULL — unless the column sets ``none_as_null``, so
rows written before that flag landed on ``model_concurrency_limits``
and ``external_attributes`` store the four-character text ``null``.
Reads treat both shapes identically (``json.loads('null')`` is
``None``), but SQL ``IS NULL`` filters miss the scalar rows. The model
now binds ``None`` as SQL NULL; this revision rewrites the legacy rows
so both storage shapes converge and the null-check predicates behave.

Revision ID: b7d3e9f1a2c4
Revises: f7a8b9c0d1e2
Create Date: 2026-11-23 19:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

from gpustack.migrations.utils import column_exists, table_exists


revision: str = 'b7d3e9f1a2c4'
down_revision: Union[str, None] = 'f7a8b9c0d1e2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_COLUMNS = ('model_concurrency_limits', 'external_attributes')


def _is_mysql_family() -> bool:
    # OceanBase reports the mysql dialect; openGauss reports postgresql,
    # so the two branches below cover all four supported stores.
    return op.get_bind().dialect.name == 'mysql'


def _scalar_null_predicate(column: str) -> str:
    # Render the stored value as text and compare: a JSON-null scalar
    # renders as 'null' on every supported store, while SQL NULL fails
    # the comparison, which keeps the UPDATE re-runnable.
    if _is_mysql_family():
        return f"CAST({column} AS CHAR) = 'null'"
    return f"{column}::text = 'null'"


def upgrade() -> None:
    if not table_exists('principals'):
        return
    for column in _COLUMNS:
        if not column_exists('principals', column):
            continue
        op.execute(
            f'UPDATE principals SET {column} = NULL '
            f'WHERE {_scalar_null_predicate(column)}'
        )


def downgrade() -> None:
    # Restores the pre-none_as_null storage shape: every missing map
    # becomes a JSON-null scalar again, which is what the old writes
    # produced and the old reads accepted.
    if not table_exists('principals'):
        return
    for column in _COLUMNS:
        if not column_exists('principals', column):
            continue
        op.execute(f"UPDATE principals SET {column} = 'null' WHERE {column} IS NULL")
