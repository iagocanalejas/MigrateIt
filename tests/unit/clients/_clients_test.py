import os
import sqlite3
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import mysql.connector
import oracledb
import psycopg
import pytest
import sqlfluff

from migrateit.clients._client import SqlClient, get_client
from migrateit.clients._connection import get_connection
from migrateit.models import Changelog, MigrateItConfig, Migration, MigrationStatus, SupportedDatabase
from tests.unit.clients.conftest import _client_with_mocked_connection
from tests.unit.conftest import INITIAL_MIGRATION, MIGRATION_NAME, TEST_MIGRATIONS_TABLE, _create_migration_file

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
        pytest.param(SupportedDatabase.POSTGRES, "psycopg.connect", id="postgres"),
        pytest.param(SupportedDatabase.MYSQL, "mysql.connector.connect", id="mysql"),
        pytest.param(SupportedDatabase.MARIADB, "mysql.connector.connect", id="mariadb"),
        pytest.param(SupportedDatabase.ORACLE, "oracledb.connect", id="oracle"),
        pytest.param(SupportedDatabase.SQLITE, "sqlite3.connect", id="sqlite"),
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
@pytest.mark.parametrize(
    "database,driver,error_class",
    [
        pytest.param(SupportedDatabase.POSTGRES, "psycopg.connect", psycopg.Error, id="postgres"),
        pytest.param(SupportedDatabase.MYSQL, "mysql.connector.connect", mysql.connector.Error, id="mysql"),
        pytest.param(SupportedDatabase.MARIADB, "mysql.connector.connect", mysql.connector.Error, id="mariadb"),
        pytest.param(SupportedDatabase.ORACLE, "oracledb.connect", oracledb.Error, id="oracle"),
        pytest.param(SupportedDatabase.SQLITE, "sqlite3.connect", sqlite3.Error, id="sqlite"),
    ],
)
def test_get_connection_error(database: SupportedDatabase, driver: str, error_class: type[Exception]) -> None:
    with patch(driver) as mock_connect:
        mock_connect.side_effect = error_class("Connection failed")
        with pytest.raises(SystemExit):
            get_connection(database)


@pytest.mark.unit
def test_get_connection_mysql_connection_string() -> None:
    with (
        patch("mysql.connector.connect") as mock_connect,
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
        patch("oracledb.connect") as mock_connect,
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

    with (
        patch("migrateit.clients.sqlite.sqlite3.connect") as mock_connect,
    ):
        SqliteClient.get_connection()
        assert mock_connect.call_args[0][0] == "migrateit.db"
        mock_connect.assert_called_once()


@pytest.mark.unit
def test_get_connection_invalid_database() -> None:
    with pytest.raises(NotImplementedError, match="is not supported"):
        get_connection("database")  # type: ignore[arg-type]


@pytest.mark.unit
def test_get_environment_url_from_db_url_psql() -> None:
    from migrateit.clients.psql import PsqlClient

    with (
        patch.dict(os.environ, {"DB_URL": "postgresql://user:pass@host:5432/mydb"}),
        patch("migrateit.clients.psql.psycopg.connect") as mock_connect,
    ):
        PsqlClient.get_connection()
        assert mock_connect.call_args.kwargs["conninfo"] == "postgresql://user:pass@host:5432/mydb"
        mock_connect.assert_called_once()


@pytest.mark.unit
def test_get_environment_url_from_db_url_mysql() -> None:
    from migrateit.clients.mysql import MySqlClient

    with (
        patch.dict(os.environ, {"DB_URL": "mysql://user:pass@host:3306/mydb"}),
        patch("migrateit.clients.mysql.mysql.connector.connect") as mock_connect,
    ):
        MySqlClient.get_connection()
        assert mock_connect.call_args.kwargs["connection_string"] == "mysql://user:pass@host:3306/mydb"
        mock_connect.assert_called_once()


@pytest.mark.unit
def test_get_environment_url_from_db_url_sqlite() -> None:
    from migrateit.clients.sqlite import SqliteClient

    with (
        patch.dict(os.environ, {"DB_URL": "sqlite:///mydb"}),
        patch("migrateit.clients.sqlite.sqlite3.connect") as mock_connect,
    ):
        SqliteClient.get_connection()
        assert mock_connect.call_args.args[0] == "mydb"
        mock_connect.assert_called_once()


@pytest.mark.unit
def test_get_environment_url_from_db_url_oracle() -> None:
    from migrateit.clients.oracle import OracleClient

    with (
        patch.dict(os.environ, {"DB_URL": "oracle+oracledb://user:pass@host:1521/service"}),
        patch("migrateit.clients.oracle.oracledb.connect") as mock_connect,
    ):
        OracleClient.get_connection()
        assert mock_connect.call_args.kwargs["dsn"] == "oracle+oracledb://user:pass@host:1521/service"
        mock_connect.assert_called_once()


@pytest.mark.unit
def test_get_environment_params_from_env_vars_oracle() -> None:
    from migrateit.clients.oracle import OracleClient

    params = {
        "DB_HOST": "oraclehost",
        "DB_PORT": "1522",
        "DB_USER": "oracleuser",
        "DB_PASS": "oraclepass",
        "DB_NAME": "oracleservice",
    }
    with (
        patch.dict(os.environ, params, clear=True),
        patch("migrateit.clients.oracle.oracledb.connect") as mock_connect,
    ):
        OracleClient.get_connection()
        kwargs = mock_connect.call_args.kwargs
        assert kwargs["host"] == "oraclehost"
        assert kwargs["port"] == 1522
        assert kwargs["user"] == "oracleuser"
        assert kwargs["password"] == "oraclepass"
        assert kwargs["service_name"] == "oracleservice"
        mock_connect.assert_called_once()


@pytest.mark.unit
def test_sql_client_none_connection_raises(temp_dir: Path) -> None:
    config = MigrateItConfig(changelog=Changelog(version=1, database=SupportedDatabase.POSTGRES))
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
    table_name = client._q(TEST_MIGRATIONS_TABLE)
    match client.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        case SupportedDatabase.ORACLE:
            import oracledb

            with client.connection.cursor() as cursor:
                try:
                    cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE")
                except oracledb.DatabaseError as e:  # pragma: no cover[safety]
                    (error,) = e.args
                    if error.code != 942:
                        raise
                cursor.execute("PURGE RECYCLEBIN")
        case SupportedDatabase.SQLITE:
            client.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        case _:  # pragma: no cover
            raise NotImplementedError
    client.connection.commit()
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
    client.config.changelog = Changelog(
        version=1,
        database=client.database,
        migrations=[Migration(name=INITIAL_MIGRATION), migration],
    )

    client.apply_migration(migration, is_fake=False)

    old_hash = client._get_database_hash(MIGRATION_NAME)

    # Update the hash
    client.update_migration_hash(migration)
    client.connection.commit()

    new_hash = client._get_database_hash(MIGRATION_NAME)

    assert old_hash == new_hash  # same file, same hash


def test_insert_already_existing(client: SqlClient[Any]) -> None:
    _create_migration_file(MIGRATION_NAME)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.config.changelog = Changelog(
        version=1,
        database=client.database,
        migrations=[Migration(name=INITIAL_MIGRATION), migration],
    )

    client.apply_migration(migration, is_fake=False)

    with pytest.raises(Exception):
        client._update_migration_changelog(migration, is_rollback=False, is_forced=False)


@pytest.mark.unit
def test_update_initial_rollback_is_nop(temp_dir: Path) -> None:
    client = _client_with_mocked_connection(temp_dir)
    migration = Migration(name="0000_init.sql", initial=True)

    client._update_migration_changelog(migration, is_rollback=True, is_forced=False)

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
