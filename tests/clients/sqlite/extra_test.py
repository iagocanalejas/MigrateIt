import os
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from migrateit.clients.sqlite import SqliteClient
from migrateit.models.changelog import Migration
from migrateit.models.migration import MigrationStatus
from migrateit.tree import create_new_migration
from tests.conftest import create_migration_file

# --- create_migrations_table tests ---


@pytest.mark.sqlite
def test_create_migrations_table(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test that the migrations table is created with correct schema."""
    del temp_dir  # unused
    table_name = "test_migrations"
    sql, rollback = SqliteClient.create_migrations_table_str(table_name)
    sqlite_client.connection.executescript(sql)

    cursor = sqlite_client.connection.execute("PRAGMA table_info(test_migrations)")
    columns = {row[1]: row[2] for row in cursor.fetchall()}
    assert "id" in columns
    assert "migration_name" in columns
    assert "applied_at" in columns
    assert "change_hash" in columns
    assert "squashed" in columns
    assert rollback is not None


# --- is_migrations_table_created tests ---


@pytest.mark.sqlite
def test_is_migrations_table_created(sqlite_client: SqliteClient) -> None:
    """Test detection of migrations table existence."""
    assert sqlite_client.is_migrations_table_created() is True

    sqlite_client.connection.execute("DROP TABLE migrations")

    assert sqlite_client.is_migrations_table_created() is False


# --- is_migration_applied tests ---


@pytest.mark.sqlite
def test_is_migration_applied(sqlite_client: SqliteClient) -> None:
    """Test checking if a specific migration is applied."""
    migration = Migration(name="0001_test.sql")
    assert sqlite_client.is_migration_applied(migration) is False

    sqlite_client.connection.execute(
        f"INSERT INTO {sqlite_client.table_name} (migration_name, change_hash) VALUES (?, ?);",
        ("0001_test.sql", "abc123"),
    )

    assert sqlite_client.is_migration_applied(migration) is True


# --- retrieve_migration_statuses tests ---


@pytest.mark.sqlite
def test_retrieve_statuses_no_table(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test retrieving statuses when no migrations table exists."""
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)

    create_new_migration(changelog=sqlite_client.changelog, migrations_dir=migrations_dir, name="migrateit")
    statuses = sqlite_client.retrieve_migration_statuses()
    assert len(statuses) == 2
    assert statuses["0000_migrateit.sql"] == MigrationStatus.NOT_APPLIED


# --- update_migration_hash tests ---


@pytest.mark.sqlite
def test_update_migration_hash(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test updating migration hash."""
    create_migration_file(
        temp_dir / "migrations",
        "0001_test.sql",
        sql="SELECT 1;",
        rollback_sql="SELECT 1;",
    )

    sqlite_client.changelog.migrations.append(
        Migration(name="0001_test.sql", initial=False, parents=["0000_migrateit.sql"])
    )

    sqlite_client.update_migration_hash(sqlite_client.changelog.migrations[1])
    sqlite_client.connection.commit()

    # Verify hash stored
    cursor = sqlite_client.connection.execute(
        f"SELECT change_hash FROM {sqlite_client.table_name} WHERE migration_name='0001_test.sql'"
    )
    row = cursor.fetchone()
    assert row is not None
    assert len(row[0]) == 64  # SHA-256 hex digest


# --- environment_url tests ---


@pytest.mark.sqlite
def test_environment_url_default() -> None:
    """Test default SQLite environment URL."""
    with patch.dict(os.environ, {}, clear=True):
        url = SqliteClient.get_environment_url()
        assert url == "sqlite:///migrateit.db"


@pytest.mark.sqlite
def test_environment_url_from_file() -> None:
    """Test SQLite environment URL from DB_FILE."""
    with patch.dict(os.environ, {"DB_FILE": "/tmp/test.db"}):
        url = SqliteClient.get_environment_url()
        assert url == "sqlite:////tmp/test.db"


@pytest.mark.sqlite
def test_environment_url_from_db_url() -> None:
    """Test SQLite environment URL from DB_URL."""
    with patch.dict(os.environ, {"DB_URL": "sqlite:///./custom.db"}):
        url = SqliteClient.get_environment_url()
        assert url == "sqlite:///./custom.db"


# --- validate_migrations tests ---


@pytest.mark.sqlite
def test_safety_table_name() -> None:
    """Test unsafe table name rejection."""
    with pytest.raises(ValueError, match="Unsafe table name"):
        SqliteClient.create_migrations_table_str("invalid-name")


@pytest.mark.sqlite
def test_validate_migrations_empty_changelog(sqlite_client: SqliteClient) -> None:
    """Test validate_migrations early returns when changelog is empty."""
    sqlite_client.changelog.migrations = []
    # Should not raise, returns early
    sqlite_client.validate_migrations({})


# --- _get_database_hash tests ---


@pytest.mark.sqlite
def test_get_database_hash_found(sqlite_client: SqliteClient) -> None:
    """Test retrieving a hash that exists."""
    sqlite_client.connection.execute(
        f"INSERT INTO {sqlite_client.table_name} (migration_name, change_hash) VALUES (?, ?);",
        ("0001_test.sql", "abc123def456"),
    )
    sqlite_client.connection.commit()
    result = sqlite_client._get_database_hash("0001_test.sql")
    assert result == "abc123def456"


@pytest.mark.sqlite
def test_get_database_hash_not_found(sqlite_client: SqliteClient) -> None:
    """Test _get_database_hash raises when migration is missing."""
    with pytest.raises(ValueError, match="not found in the database"):
        sqlite_client._get_database_hash("nonexistent.sql")


# --- apply_migration exception path ---


@pytest.mark.sqlite
def test_apply_migration_rollback_on_sqlite_error(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test that SQLite errors trigger a rollback."""
    create_migration_file(
        temp_dir / "migrations",
        "0001_bad.sql",
        sql="NOT VALID SQL AT ALL !!!",
        rollback_sql="SELECT 1;",
    )
    sqlite_client.changelog.migrations.append(
        Migration(name="0001_bad.sql", initial=False, parents=["0000_migrateit.sql"])
    )
    migration = sqlite_client.changelog.migrations[1]
    with pytest.raises(sqlite3.Error):
        sqlite_client.apply_migration(migration)
