import os
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlfluff

from migrateit.clients._client import SqlClient
from migrateit.models.changelog import ChangelogFile, SupportedDatabase
from migrateit.models.config import MigrateItConfig
from migrateit.models.connection import get_connection
from migrateit.models.migration import Migration, MigrationStatus
from tests.conftest import INITIAL_MIGRATION, TEST_MIGRATIONS_TABLE, _create_migration_file, _drop_test_table

MIGRATION_NAME = "0001_test_table.sql"

# --- get_connection tests ---


@pytest.mark.unit
@pytest.mark.parametrize(
    "database,driver",
    [
        pytest.param(SupportedDatabase.POSTGRES, "migrateit.models.connection.psycopg.connect", id="postgres"),
        pytest.param(SupportedDatabase.MYSQL, "migrateit.models.connection.mysql.connector.connect", id="mysql"),
        pytest.param(SupportedDatabase.MARIADB, "migrateit.models.connection.mysql.connector.connect", id="mariadb"),
    ],
)
def test_get_connection(database: SupportedDatabase, driver: str) -> None:
    with patch(driver) as mock_connect:
        mock_conn: Any = MagicMock()
        mock_connect.return_value = mock_conn
        result = get_connection(database)
        assert result == mock_conn
        mock_connect.assert_called_once()


@pytest.mark.unit
def test_get_connection_sqlite() -> None:
    from migrateit.clients.sqlite import SqliteClient

    # With SQLITE we can create a real connection
    f = Path(SqliteClient.get_connection_params()["file_name"])
    result = get_connection(SupportedDatabase.SQLITE)
    assert isinstance(result, sqlite3.Connection)
    result.close()
    f.unlink()


@pytest.mark.unit
def test_get_connection_mysql_connection_string() -> None:
    with (
        patch("migrateit.models.connection.mysql.connector.connect") as mock_connect,
        patch.dict(os.environ, {"DB_URL": "mysql://user:pass@host:3306/mydb"}),
    ):
        mock_conn: Any = MagicMock()
        mock_connect.return_value = mock_conn
        result = get_connection(SupportedDatabase.MYSQL)
        assert result == mock_conn
        mock_connect.assert_called_once()


@pytest.mark.unit
def test_get_environment_url_from_db_url_psql() -> None:
    from migrateit.clients.psql import PsqlClient

    with patch.dict(os.environ, {"DB_URL": "postgresql://user:pass@host:5432/mydb"}):
        url = PsqlClient.get_connection_params()["conninfo"]
        assert url == "postgresql://user:pass@host:5432/mydb"


@pytest.mark.unit
def test_get_environment_url_from_db_url_mysql() -> None:
    from migrateit.clients.mysql import MySqlClient

    with patch.dict(os.environ, {"DB_URL": "mysql://user:pass@host:3306/mydb"}):
        url = MySqlClient.get_connection_params()["connection_string"]
        assert url == "mysql://user:pass@host:3306/mydb"


@pytest.mark.unit
def test_get_environment_url_from_db_url_sqlite() -> None:
    from migrateit.clients.sqlite import SqliteClient

    with patch.dict(os.environ, {"DB_URL": "sqlite:///mydb"}):
        url = SqliteClient.get_connection_params()["url"]
        assert url == "sqlite:///mydb"


@pytest.mark.unit
def test_sql_client_none_connection_raises(temp_dir: Path) -> None:
    config = MigrateItConfig(
        table_name="migrations",
        migrations_dir=temp_dir,
        changelog=ChangelogFile(version=1, path=temp_dir / "changelog.json"),
    )
    with pytest.raises(ValueError, match="connection cannot be None"):
        SqlClient[None](None, config)  # type: ignore


# --- config tests ---


@pytest.mark.unit
def test_sql_client_valid_config(temp_dir: Path) -> None:
    config = MigrateItConfig(
        table_name="migrations",
        migrations_dir=temp_dir,
        changelog=ChangelogFile(version=1, path=temp_dir / "changelog.json"),
    )
    mock_conn = MagicMock()
    client: SqlClient[MagicMock] = SqlClient(mock_conn, config)  # type: ignore[abstract]
    assert client.config == config
    assert client.connection == mock_conn
    assert client.table_name == "migrations"
    assert client.migrations_dir == temp_dir
    assert isinstance(client.changelog, ChangelogFile)


@pytest.mark.unit
@pytest.mark.parametrize(
    "table_name,expected_error,expected_match",
    [
        pytest.param("", ValueError, "Table name is required", id="empty"),
        pytest.param(123, TypeError, None, id="non_string"),
        pytest.param("invalid-name", ValueError, "valid identifier", id="invalid_identifier"),
    ],
)
def test_validate_config(table_name: object, expected_error: type[Exception], expected_match: str | None) -> None:
    config = MigrateItConfig(
        table_name=table_name,  # type: ignore[arg-type]
        migrations_dir=Path("/tmp/migrations"),
        changelog=ChangelogFile(version=1, path=Path("/tmp/changelog.json")),
    )
    with pytest.raises(expected_error, match=expected_match):
        SqlClient.validate_config(config)


# --- create_migrations_table tests ---


def test_create_migrations_table(client: SqlClient[Any]) -> None:
    """Test that the migrations table SQL is generated correctly."""
    table_name = "test_migrations"
    sql, rollback = client.create_migrations_table_str(table_name)

    lint_errors = sqlfluff.lint(sql, dialect=client.changelog.database.value)
    assert not [e for e in lint_errors if e["code"] == "PRS"]

    lint_errors = sqlfluff.lint(rollback, dialect=client.changelog.database.value)
    assert not [e for e in lint_errors if e["code"] == "PRS"]


def test_safety_table_name(client: SqlClient[Any]) -> None:
    """Test unsafe table name rejection."""
    with pytest.raises(ValueError, match="Unsafe table name"):
        client.create_migrations_table_str("invalid-name")


# --- is_migrations_table_created tests ---


def test_table_exists_returns_true(client: SqlClient[Any]) -> None:
    # table is created in the fixture
    assert client.is_migrations_table_created()


def test_table_missing_returns_false(client: SqlClient[Any]) -> None:
    # drop the table first as is being created in pytest fixtures
    _drop_test_table(client, TEST_MIGRATIONS_TABLE)
    assert not client.is_migrations_table_created()


# --- is_migration_applied tests ---


def test_applied_migration_returns_true(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(migrations_dir, MIGRATION_NAME)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    client.apply_migration(migration, is_fake=False)
    assert client.is_migration_applied(migration)


def test_applied_migration_returns_true_for_fake(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(migrations_dir, MIGRATION_NAME)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    client.apply_migration(migration, is_fake=True)
    assert client.is_migration_applied(migration)


def test_not_applied_migration_returns_false(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(migrations_dir, MIGRATION_NAME)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    assert not client.is_migration_applied(migration)


# --- retrieve_migration_statuses tests ---


def test_no_table_returns_not_applied(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _drop_test_table(client, TEST_MIGRATIONS_TABLE)

    _create_migration_file(migrations_dir, "0001_test.sql")
    migrations = [Migration(name="0001_test.sql", parents=(INITIAL_MIGRATION,))]
    client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INITIAL_MIGRATION), *migrations])

    statuses = client.retrieve_migration_statuses()
    assert statuses["0001_test.sql"] == MigrationStatus.NOT_APPLIED


# --- update_migration_hash tests ---


def test_update_migration_hash(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(migrations_dir, MIGRATION_NAME)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INITIAL_MIGRATION), migration])

    client.apply_migration(migration, is_fake=False)

    old_hash = client._get_database_hash(MIGRATION_NAME)

    # Update the hash
    client.update_migration_hash(migration)
    client.connection.commit()

    new_hash = client._get_database_hash(MIGRATION_NAME)

    assert old_hash == new_hash  # same file, same hash


# --- _get_database_hash tests ---


def test_get_database_hash_not_found(client: SqlClient[Any]) -> None:
    with pytest.raises(ValueError, match="not found in the database"):
        client._get_database_hash("nonexistent.sql")
