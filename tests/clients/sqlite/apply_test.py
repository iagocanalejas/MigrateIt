import os
from pathlib import Path

import pytest

from migrateit.clients.sqlite import SqliteClient
from migrateit.models import Migration
from migrateit.models.migration import MigrationStatus
from tests.conftest import create_migration_file


@pytest.mark.sqlite
def test_apply_migration_success(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test applying a migration and checking status."""
    create_migration_file(
        temp_dir / "migrations",
        "0001_create_users.sql",
        sql="CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        rollback_sql="DROP TABLE users;",
    )

    sqlite_client.changelog.migrations.append(
        Migration(name="0001_create_users.sql", initial=False, parents=["0000_migrateit.sql"])
    )

    migration = sqlite_client.changelog.migrations[1]
    sqlite_client.apply_migration(migration)
    sqlite_client.connection.commit()

    statuses = sqlite_client.retrieve_migration_statuses()
    assert statuses["0001_create_users.sql"] == MigrationStatus.APPLIED

    cursor = sqlite_client.connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    assert cursor.fetchone() is not None


@pytest.mark.sqlite
def test_apply_migration_fake(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test fake applying a migration."""
    create_migration_file(
        temp_dir / "migrations",
        "0001_create_items.sql",
        sql="CREATE TABLE items (id INTEGER PRIMARY KEY, title TEXT);",
        rollback_sql="DROP TABLE items;",
    )

    sqlite_client.changelog.migrations.append(
        Migration(name="0001_create_items.sql", initial=False, parents=["0000_migrateit.sql"])
    )

    migration = sqlite_client.changelog.migrations[1]
    sqlite_client.apply_migration(migration, is_fake=True)
    sqlite_client.connection.commit()

    cursor = sqlite_client.connection.execute("SELECT migration_name FROM migrations")
    names = {row[0] for row in cursor.fetchall()}
    assert "0001_create_items.sql" in names

    # Verify table was NOT created
    cursor = sqlite_client.connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
    assert cursor.fetchone() is None


@pytest.mark.sqlite
def test_apply_migration_undo_success(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test rolling back a migration."""
    create_migration_file(
        temp_dir / "migrations",
        "0001_create_table.sql",
        sql="CREATE TABLE test (id INTEGER PRIMARY KEY);",
        rollback_sql="DROP TABLE test;",
    )

    sqlite_client.changelog.migrations.append(
        Migration(name="0001_create_table.sql", initial=False, parents=["0000_migrateit.sql"])
    )

    migration = sqlite_client.changelog.migrations[1]
    sqlite_client.apply_migration(migration)
    sqlite_client.connection.commit()

    # Verify applied
    cursor = sqlite_client.connection.execute("SELECT migration_name FROM migrations")
    names = {row[0] for row in cursor.fetchall()}
    assert "0001_create_table.sql" in names

    # Rollback
    sqlite_client.apply_migration(migration, is_rollback=True)
    sqlite_client.connection.commit()

    # Verify rolled back
    cursor = sqlite_client.connection.execute("SELECT migration_name FROM migrations")
    names = {row[0] for row in cursor.fetchall()}
    assert "0001_create_table.sql" not in names


@pytest.mark.sqlite
def test_apply_migration_undo_fake(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test fake rolling back a migration."""
    create_migration_file(
        temp_dir / "migrations",
        "0001_fake_table.sql",
        sql="CREATE TABLE fake (id INTEGER);",
        rollback_sql="DROP TABLE fake;",
    )

    sqlite_client.changelog.migrations.append(
        Migration(name="0001_fake_table.sql", initial=False, parents=["0000_migrateit.sql"])
    )

    migration = sqlite_client.changelog.migrations[1]
    sqlite_client.apply_migration(migration, is_fake=True)
    sqlite_client.connection.commit()

    sqlite_client.apply_migration(migration, is_fake=True, is_rollback=True)
    sqlite_client.connection.commit()

    cursor = sqlite_client.connection.execute("SELECT migration_name FROM migrations")
    names = {row[0] for row in cursor.fetchall()}
    assert "0001_fake_table.sql" not in names


@pytest.mark.sqlite
def test_apply_migration_file_missing(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test applying a migration with missing file."""
    migration = Migration(name="not_found.sql", parents=["0000_migrateit.sql"])

    with pytest.raises(FileNotFoundError):
        sqlite_client.apply_migration(migration, is_fake=False)


@pytest.mark.sqlite
def test_apply_migration_wrong_extension(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test applying a migration with wrong extension."""
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0002_wrong_ext.txt"
    path = migrations_dir / filename
    path.write_text("SELECT 1;")

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    sqlite_client.changelog.migrations.append(migration)

    with pytest.raises(FileNotFoundError):
        sqlite_client.apply_migration(migration, is_fake=False)


@pytest.mark.sqlite
def test_apply_migration_already_applied(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test applying an already applied migration."""
    create_migration_file(
        temp_dir / "migrations",
        "0001_applied.sql",
        sql="SELECT 1;",
        rollback_sql="SELECT 1;",
    )

    sqlite_client.changelog.migrations.append(
        Migration(name="0001_applied.sql", initial=False, parents=["0000_migrateit.sql"])
    )

    migration = sqlite_client.changelog.migrations[1]
    sqlite_client.apply_migration(migration)
    sqlite_client.connection.commit()

    with pytest.raises(ValueError):
        sqlite_client.apply_migration(migration)
