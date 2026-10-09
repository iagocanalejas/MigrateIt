from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from migrateit.clients._client import SqlClient
from migrateit.models.migration import Migration, MigrationStatus
from tests.clients._clients_test import MIGRATION_NAME
from tests.conftest import (
    INITIAL_MIGRATION,
    TEST_MIGRATIONS_TABLE,
    TEST_TABLE,
    _create_migration_file,
    _drop_test_table,
    _migration_is_applied,
    _table_exists,
)


def test_apply_migration_success(client: SqlClient[Any], temp_dir: Path) -> None:
    """Test applying a migration and checking it's recorded in the DB."""
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )

    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    client.apply_migration(migration)

    statuses = client.retrieve_migration_statuses()
    assert statuses[MIGRATION_NAME] == MigrationStatus.APPLIED
    assert _migration_is_applied(client, MIGRATION_NAME)
    assert _table_exists(client, TEST_TABLE)

    _drop_test_table(client)


def test_apply_migration_fake(client: SqlClient[Any], temp_dir: Path) -> None:
    """Test fake applying a migration (recorded but SQL not executed)."""
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )

    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    client.apply_migration(migration, is_fake=True)

    statuses = client.retrieve_migration_statuses()
    assert statuses[MIGRATION_NAME] == MigrationStatus.APPLIED
    assert _migration_is_applied(client, MIGRATION_NAME)
    assert not _table_exists(client, TEST_TABLE)


def test_rollback_migration_success(client: SqlClient[Any], temp_dir: Path) -> None:
    """Test rolling back a migration."""
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql=f"CREATE TABLE IF NOT EXISTS {TEST_TABLE} (id SERIAL PRIMARY KEY);",
        rollback_sql=f"DROP TABLE IF EXISTS {TEST_TABLE};",
    )

    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    client.apply_migration(migration, is_fake=False)
    client.apply_migration(migration, is_fake=False, is_rollback=True)

    statuses = client.retrieve_migration_statuses()
    assert statuses[MIGRATION_NAME] == MigrationStatus.NOT_APPLIED
    assert not _migration_is_applied(client, MIGRATION_NAME)
    assert not _table_exists(client, TEST_TABLE)


def test_rollback_migration_initial(client: SqlClient[Any], temp_dir: Path) -> None:
    """Test rolling back a migration."""

    client.apply_migration(client.changelog.root, is_rollback=True)

    statuses = client.retrieve_migration_statuses()
    assert statuses[INITIAL_MIGRATION] == MigrationStatus.NOT_APPLIED
    assert not _table_exists(client, TEST_MIGRATIONS_TABLE)


def test_rollback_fake_migration(client: SqlClient[Any], temp_dir: Path) -> None:
    """Test fake applying then rollback."""
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(migrations_dir, MIGRATION_NAME)

    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    client.apply_migration(migration, is_fake=True)
    client.apply_migration(migration, is_rollback=True)

    statuses = client.retrieve_migration_statuses()
    assert statuses[MIGRATION_NAME] == MigrationStatus.NOT_APPLIED
    assert not _migration_is_applied(client, MIGRATION_NAME)
    assert not _table_exists(client, TEST_TABLE)


def test_fake_rollback_migration(client: SqlClient[Any], temp_dir: Path) -> None:
    """Test fake rolling back a migration."""
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )

    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    client.apply_migration(migration)
    client.apply_migration(migration, is_fake=True, is_rollback=True)
    assert not _migration_is_applied(client, MIGRATION_NAME)
    assert _table_exists(client, TEST_TABLE)


def test_apply_migration_already_applied(client: SqlClient[Any], temp_dir: Path) -> None:
    """Test applying an already applied migration raises ValueError."""
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(migrations_dir, MIGRATION_NAME)

    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    client.apply_migration(migration, is_fake=False)
    with pytest.raises(ValueError, match="already applied, cannot apply it again"):
        client.apply_migration(migration, is_fake=False)


def test_apply_migration_file_missing(client: SqlClient[Any]) -> None:
    """Test applying a migration with a missing file."""
    migration = Migration(name="not_found.sql", parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    with pytest.raises(FileNotFoundError):
        client.apply_migration(migration, is_fake=False)


def test_rollback_migration_error(client: SqlClient[Any], temp_dir: Path) -> None:
    """Test that errors trigger a rollback."""
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(migrations_dir, MIGRATION_NAME, sql="INVALID SQL")

    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    spy_connection = MagicMock(wraps=client.connection)
    client.connection = spy_connection

    with pytest.raises(Exception):
        client.apply_migration(migration)
    spy_connection.rollback.assert_called_once()


def test_rollback_migration_not_applied(client: SqlClient[Any], temp_dir: Path) -> None:
    """Test rolling back a migration that was never applied raises ValueError."""
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(migrations_dir, MIGRATION_NAME)

    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    with pytest.raises(ValueError, match="is not applied, cannot undo it"):
        client.apply_migration(migration, is_fake=False, is_rollback=True)
