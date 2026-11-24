"""The ``limit_event_count`` migration adds the rejection-count column and
backfills the rate-limit rows from their spec snapshots."""

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
    / '2026_11_24_1300-f3a9b5c1d7e8_limit_event_count.py'
)
spec = importlib.util.spec_from_file_location('limit_event_count_migration', MIGRATION)
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
    'resource_events',
    METADATA,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('occurred_at', sa.DateTime),
    sa.Column('resource_type', sa.String),
    sa.Column('resource_name', sa.String),
    sa.Column('event_type', sa.String),
    sa.Column('event_message', sa.String),
    sa.Column('spec_snapshot', sa.JSON),
)

OCCURRED = datetime(2026, 11, 24, 13, 30, 0)


@pytest.fixture
def seeded(monkeypatch):
    engine, conn, op = _bound(monkeypatch)
    try:
        # The archive table is deliberately absent — the revision must not
        # require it (older deployments may not have run its creator yet).
        TABLE.create(conn)
        conn.execute(
            TABLE.insert(),
            [
                # A folded row: the snapshot count is authoritative.
                {
                    'id': 1,
                    'occurred_at': OCCURRED,
                    'resource_type': 'limit',
                    'resource_name': 'alice',
                    'event_type': 'limit_rate_requests',
                    'event_message': ('Too many requests per minute, +45 suppressed'),
                    'spec_snapshot': {'count': 46},
                },
                # A weekly row written before the split from TPM.
                {
                    'id': 2,
                    'occurred_at': OCCURRED,
                    'resource_type': 'limit',
                    'resource_name': 'bob',
                    'event_type': 'limit_rate_tokens',
                    'event_message': 'Weekly token quota exceeded',
                    'spec_snapshot': {'count': 3},
                },
                # A lifecycle row: no count, no snapshot — stays NULL.
                {
                    'id': 3,
                    'occurred_at': OCCURRED,
                    'resource_type': 'gpu_instance',
                    'resource_name': 'gpu-1',
                    'event_type': 'created',
                    'event_message': 'GPU instance created',
                    'spec_snapshot': None,
                },
                # A limit row with neither snapshot nor parseable message.
                {
                    'id': 4,
                    'occurred_at': OCCURRED,
                    'resource_type': 'limit',
                    'resource_name': 'carol',
                    'event_type': 'limit_concurrency',
                    'event_message': 'Too many concurrent requests for this model',
                    'spec_snapshot': None,
                },
            ],
        )
        conn.commit()
        yield conn
    finally:
        engine.dispose()


def _rows(conn):
    return {
        row['id']: row
        for row in conn.execute(
            sa.text('SELECT id, event_type, event_count FROM resource_events')
        ).mappings()
    }


def test_upgrade_backfills_counts_and_retypes_weekly(monkeypatch, seeded):
    migration.upgrade()
    rows = _rows(seeded)

    assert rows[1]['event_count'] == 46
    # The weekly row splits from the per-minute TPM axis it used to share.
    assert rows[2]['event_type'] == 'limit_quota_weekly'
    assert rows[2]['event_count'] == 3
    # Lifecycle rows carry no count — aggregation treats NULL as 1.
    assert rows[3]['event_count'] is None
    # No parseable tail: exactly one rejection.
    assert rows[4]['event_count'] == 1


def test_upgrade_is_re_runnable(monkeypatch, seeded):
    migration.upgrade()
    migration.upgrade()
    rows = _rows(seeded)
    assert rows[1]['event_count'] == 46
    assert rows[2]['event_type'] == 'limit_quota_weekly'


def test_upgrade_without_the_hot_table(monkeypatch):
    engine, conn, _op = _bound(monkeypatch)
    try:
        # No tables at all — the revision must be a no-op, not a crash.
        migration.upgrade()
    finally:
        engine.dispose()
