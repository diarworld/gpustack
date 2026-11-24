"""The ``reset_outcome_columns`` migration adds the decision-model verdict
columns and backfills existing rows as approvals."""

import importlib.util
from datetime import datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / 'gpustack/migrations/versions'
    / '2026_11_25_1100-e9c2a6d4b7f1_reset_outcome_columns.py'
)
spec = importlib.util.spec_from_file_location('reset_outcome_migration', MIGRATION)
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


def _bound(monkeypatch):
    """An alembic Operations bound to a fresh sqlite connection, wired
    into the migration module and the shared ``table_exists`` helper the
    way a real revision run is."""
    engine = sa.create_engine('sqlite://')
    conn = engine.connect()
    op = Operations(MigrationContext.configure(conn))
    monkeypatch.setattr(migration, 'op', op)
    monkeypatch.setattr('gpustack.migrations.utils.op', op)
    return engine, conn, op


METADATA = sa.MetaData()

TABLE = sa.Table(
    'gateway_rate_limit_resets',
    METADATA,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('user_id', sa.Integer),
    sa.Column('kind', sa.String),
    sa.Column('reset_at', sa.DateTime),
    sa.Column('reset_by_user_id', sa.Integer),
    sa.Column('description', sa.Text),
)


@pytest.fixture
def seeded(monkeypatch):
    engine, conn, op = _bound(monkeypatch)
    try:
        TABLE.create(conn)
        conn.execute(
            TABLE.insert().values(
                user_id=7,
                kind='weekly',
                reset_at=datetime(2026, 11, 24, 12, 0, 0),
                reset_by_user_id=None,
                description='backfill job',
            )
        )
        conn.execute(
            TABLE.insert().values(
                user_id=8,
                kind='monthly',
                reset_at=datetime(2026, 11, 24, 13, 0, 0),
                reset_by_user_id=1,
                description='support',
            )
        )
        yield conn
    finally:
        engine.dispose()


def test_upgrade_adds_columns_and_backfills_outcome(seeded):
    """Existing rows were unconditional approvals: the new outcome column
    states that, and the verdict columns land nullable."""
    migration.upgrade()
    conn = seeded

    rows = conn.execute(
        sa.text(
            'SELECT user_id, outcome, work_type, rejection_reason '
            'FROM gateway_rate_limit_resets ORDER BY user_id'
        )
    ).fetchall()
    assert [(r[0], r[1], r[2], r[3]) for r in rows] == [
        (7, 'approved', None, None),
        (8, 'approved', None, None),
    ]


def test_upgrade_is_re_runnable(seeded):
    """Running the revision against an already-migrated table changes
    nothing: the column guards are the whole idempotency story."""
    migration.upgrade()
    migration.upgrade()

    columns = {
        row[1]
        for row in seeded.execute(
            sa.text('PRAGMA table_info(gateway_rate_limit_resets)')
        ).fetchall()
    }
    assert {'outcome', 'work_type', 'rejection_reason'} <= columns


def test_downgrade_drops_only_the_verdict_columns(seeded):
    """The downgrade removes the verdict columns but leaves the reset
    history (rows, description) intact."""
    migration.upgrade()
    migration.downgrade()

    columns = {
        row[1]
        for row in seeded.execute(
            sa.text('PRAGMA table_info(gateway_rate_limit_resets)')
        ).fetchall()
    }
    assert not columns & {'outcome', 'work_type', 'rejection_reason'}
    count = seeded.execute(
        sa.text('SELECT count(*) FROM gateway_rate_limit_resets')
    ).scalar()
    assert count == 2
