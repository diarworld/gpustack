"""The ``gateway_rate_limit_settings`` migration builds a re-runnable
singleton table."""

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / 'gpustack/migrations/versions'
    / '2026_11_21_2130-e8a2c4d6f913_gateway_rate_limit_settings.py'
)
spec = importlib.util.spec_from_file_location(
    'gateway_rate_limit_settings_migration', MIGRATION
)
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)

EXPECTED_COLUMNS = {
    'id',
    'default_rpm',
    'default_tpm',
    'default_monthly_tokens',
    'default_concurrency',
    'free_enabled',
    'free_start',
    'free_end',
    'free_days',
    'deleted_at',
    'created_at',
    'updated_at',
}


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


def _columns(op):
    return {
        c['name']
        for c in sa.inspect(op.get_bind()).get_columns('gateway_rate_limit_settings')
    }


def test_upgrade_creates_the_singleton_shape(monkeypatch):
    engine, conn, op = _bound(monkeypatch)
    try:
        migration.upgrade()
        assert _columns(op) == EXPECTED_COLUMNS
    finally:
        conn.close()
        engine.dispose()


def test_upgrade_is_re_runnable(monkeypatch):
    """A partially applied state (the table already there) must be a
    no-op, not a crash: the guard is what lets a failed rollout be
    retried."""
    engine, conn, op = _bound(monkeypatch)
    try:
        migration.upgrade()
        migration.upgrade()
        assert _columns(op) == EXPECTED_COLUMNS
    finally:
        conn.close()
        engine.dispose()


def test_downgrade_drops_the_table(monkeypatch):
    engine, conn, op = _bound(monkeypatch)
    try:
        migration.upgrade()
        migration.downgrade()
        assert not sa.inspect(op.get_bind()).has_table('gateway_rate_limit_settings')
    finally:
        conn.close()
        engine.dispose()
