import os
import sqlite3
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlfluff

from migrateit.clients._client import SqlClient, get_client
from migrateit.models.changelog import Changelog
from migrateit.models.config import MigrateItConfig, SupportedDatabase
from migrateit.models.connection import get_connection
from migrateit.models.migration import Migration, MigrationStatus
from tests.unit.clients.conftest import _client_with_mocked_connection, _drop_test_table
from tests.unit.conftest import (
    INITIAL_MIGRATION,
    MIGRATION_NAME,
    TEST_MIGRATIONS_TABLE,
    _create_migration_file,
)

# --- _q — identifier quoting ---


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "quote_char"),
    [
        pytest.param('my"table', '"', id="quote_in_name"),
        pytest.param("\x00null", '"', id="null_byte"),
    ],
)
def test_q_invalid_name(name: str, quote_char: str) -> None:
    with pytest.raises(ValueError, match="Invalid identifier"):
        SqlClient._q(name, quote_char)


# --- get_connection tests ---


@pytest.mark.unit
@pytest.mark.parametrize(
    "database,driver",
    [
        pytest.param(SupportedDatabase.POSTGRES, "migrateit.models.connection.psycopg.connect", id="postgres"),
        pytest.param(SupportedDatabase.MYSQL, "migrateit.models.connection.mysql.connector.connect", id="mysql"),
        pytest.param(SupportedDatabase.MARIADB, "migrateit.models.connection.mysql.connector.connect", id="mariadb"),
        pytest.param(SupportedDatabase.ORACLE, "migrateit.models.connection.oracledb.connect", id="oracle"),
    ],
)
def test_get_connection(database: SupportedDatabase, driver: str) -> None:
    mock_conn: Any = MagicMock()
    with patch(driver) as mock_connect:
        mock_connect.return_value = mock_conn
        result = get_connection(database)
        assert result == mock_conn
        mock_connect.assert_called_once()


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
def test_get_connection_oracle_connection_string() -> None:
    with (
        patch("migrateit.models.connection.oracledb.connect") as mock_connect,
        patch.dict(os.environ, {"DB_URL": "oracle+oracledb://user:pass@host:1521/service"}),
    ):
        mock_conn: Any = MagicMock()
        mock_connect.return_value = mock_conn
        result = get_connection(SupportedDatabase.ORACLE)
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
def test_get_connection_invalid_database() -> None:
    with pytest.raises(NotImplementedError, match="is not supported"):
        get_connection("database")  # type: ignore[arg-type]


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
def test_get_environment_url_from_db_url_oracle() -> None:
    from migrateit.clients.oracle import OracleClient

    with patch.dict(os.environ, {"DB_URL": "oracle+oracledb://user:pass@host:1521/service"}):
        params = OracleClient.get_connection_params()
        assert params["dsn"] == "oracle+oracledb://user:pass@host:1521/service"


@pytest.mark.unit
def test_get_environment_params_from_env_vars_oracle() -> None:
    from migrateit.clients.oracle import OracleClient

    with patch.dict(
        os.environ,
        {
            "DB_HOST": "oraclehost",
            "DB_PORT": "1522",
            "DB_USER": "oracleuser",
            "DB_PASS": "oraclepass",
            "DB_NAME": "oracleservice",
        },
        clear=True,
    ):
        params = OracleClient.get_connection_params()
        assert params["host"] == "oraclehost"
        assert params["port"] == 1522
        assert params["user"] == "oracleuser"
        assert params["password"] == "oraclepass"
        assert params["service_name"] == "oracleservice"


@pytest.mark.unit
def test_sql_client_none_connection_raises(temp_dir: Path) -> None:
    config = MigrateItConfig(changelog=Changelog(version=1))
    with pytest.raises(ValueError, match="connection cannot be None"):
        SqlClient[None](None, config)  # type: ignore


# --- create_migrations_table tests ---


def test_create_migrations_table(client: SqlClient[Any]) -> None:
    def _sql_to_statements(sql: str) -> Generator[str]:
        for statement in sql.split(";"):
            stmt = statement.strip()
            if stmt:
                yield stmt

    sql, rollback = client.create_migrations_table_str("create_migration_table")

    for stmt in _sql_to_statements(sql):
        lint_errors = sqlfluff.lint(stmt, dialect=client.database.value)
        assert not [e for e in lint_errors if e["code"] == "PRS"]

    for stmt in _sql_to_statements(rollback):
        lint_errors = sqlfluff.lint(stmt, dialect=client.database.value)
        assert not [e for e in lint_errors if e["code"] == "PRS"]


def test_safety_table_name(client: SqlClient[Any]) -> None:
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


def test_applied_migration_returns_true(client: SqlClient[Any]) -> None:
    _create_migration_file(MIGRATION_NAME)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    client.apply_migration(migration, is_fake=False)
    assert client.is_migration_applied(migration)


def test_applied_migration_returns_true_for_fake(client: SqlClient[Any]) -> None:
    _create_migration_file(MIGRATION_NAME)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    client.apply_migration(migration, is_fake=True)
    assert client.is_migration_applied(migration)


def test_not_applied_migration_returns_false(client: SqlClient[Any]) -> None:
    _create_migration_file(MIGRATION_NAME)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    assert not client.is_migration_applied(migration)


# --- retrieve_migration_statuses tests ---


@pytest.mark.unit
def test_no_table_returns_not_applied(temp_dir: Path) -> None:
    client = _client_with_mocked_connection(temp_dir)

    # initial not applied
    statuses = client.retrieve_migration_statuses()
    assert statuses[INITIAL_MIGRATION] == MigrationStatus.NOT_APPLIED


@pytest.mark.unit
def test_retrieve_migration_statuses_hash_mismatch(temp_dir: Path) -> None:
    client = _client_with_mocked_connection(temp_dir)
    client.connection.execute.return_value.fetchone.return_value = [True]  # is_migrations_table_created
    client.connection.execute.return_value.fetchall.return_value = [("0001_test.sql", "db_hash")]

    _create_migration_file("0001_test.sql")
    client.config.changelog.migrations.append(Migration(name="0001_test.sql", parents=(INITIAL_MIGRATION,)))

    # return a different hash than the DB
    with patch.object(Migration, "hash", return_value="file_hash"):
        statuses = client.retrieve_migration_statuses()
    assert statuses["0001_test.sql"] == MigrationStatus.CONFLICT


@pytest.mark.unit
def test_retrieve_migration_statuses_removed(temp_dir: Path) -> None:
    client = _client_with_mocked_connection(temp_dir)
    client.connection.execute.return_value.fetchone.return_value = [True]
    client.connection.execute.return_value.fetchall.return_value = [("ghost.sql", "some_hash")]

    # ghost.sql appears because it's in the DB but not in the changelog
    statuses = client.retrieve_migration_statuses()
    assert statuses["ghost.sql"] == MigrationStatus.REMOVED


# --- update_migration_hash tests ---


def test_update_migration_hash(client: SqlClient[Any]) -> None:
    _create_migration_file(MIGRATION_NAME)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.config.changelog = Changelog(version=1, migrations=[Migration(name=INITIAL_MIGRATION), migration])

    client.apply_migration(migration, is_fake=False)

    old_hash = client._get_database_hash(MIGRATION_NAME)

    # Update the hash
    client.update_migration_hash(migration)
    client.connection.commit()

    new_hash = client._get_database_hash(MIGRATION_NAME)

    assert old_hash == new_hash  # same file, same hash


@pytest.mark.unit
def test_update_initial_rollback_is_nop(temp_dir: Path) -> None:
    client = _client_with_mocked_connection(temp_dir)
    migration = Migration(name="0000_init.sql", initial=True)

    client._update_migration_changelog(migration, is_rollback=True)

    client.connection.cursor.return_value.execute.assert_not_called()


# --- _get_database_hash tests ---


def test_get_database_hash_not_found(client: SqlClient[Any]) -> None:
    with pytest.raises(ValueError, match="not found in the database"):
        client._get_database_hash("nonexistent.sql")


# --- get_client tests ---


@pytest.mark.unit
def test_get_client_no_supported_database() -> None:
    config = MigrateItConfig(
        changelog=Changelog(
            version=1,
            migrations=[],
            database="unsupported",  # type: ignore[arg-type]
        ),
    )
    with pytest.raises(NotImplementedError, match="not supported"):
        get_client(config, MagicMock())
