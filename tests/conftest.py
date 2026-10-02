import sqlite3
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch
from urllib.parse import urlparse

import pytest

from migrateit.clients._client import SqlClient, get_client
from migrateit.clients.sqlite import SqliteClient
from migrateit.cmd import cmd_init
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.config import MigrateItConfig
from migrateit.tree import ROLLBACK_SPLIT_TAG, load_changelog_file

INITIAL_MIGRATION = "0000_migrateit.sql"
TEST_MIGRATIONS_TABLE = "migrations"
TEST_TABLE = "test_entity"


@pytest.fixture(autouse=True)
def suppress_write() -> Generator[None]:
    with (
        patch("migrateit.reporters.output.write_line_b", lambda *_: None),
        patch("migrateit.reporters.output.write_line", lambda *_: None),
    ):
        yield


@pytest.fixture()
def temp_dir() -> Generator[Path]:
    """Provides a temporary directory as a string path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@contextmanager
def connection(database_type: SupportedDatabase) -> Generator[Any]:
    match database_type:
        case SupportedDatabase.POSTGRES:
            import psycopg

            from migrateit.clients.psql import PsqlClient

            pg_conn: psycopg.Connection = psycopg.connect(PsqlClient.get_environment_url())
            try:
                yield pg_conn
            finally:
                with pg_conn.cursor() as cursor:
                    cursor.execute("DROP SCHEMA public CASCADE;")
                    cursor.execute("DROP SCHEMA IF EXISTS app_schema CASCADE;")
                    cursor.execute("CREATE SCHEMA public;")
                pg_conn.commit()
                pg_conn.close()

        case SupportedDatabase.MYSQL:
            import mysql.connector

            from migrateit.clients.mysql import MySqlClient

            parsed = urlparse(MySqlClient.get_environment_url())
            db_name = parsed.path.lstrip("/")
            mysql_conn = mysql.connector.connect(
                host=parsed.hostname,
                port=parsed.port,
                user=parsed.username,
                password=parsed.password,
                database=db_name,
            )
            try:
                yield mysql_conn
            finally:
                with mysql_conn.cursor() as cursor:
                    cursor.execute(f"DROP DATABASE IF EXISTS `{db_name}`;")
                    cursor.execute(f"CREATE DATABASE `{db_name}`;")
                mysql_conn.commit()
                mysql_conn.close()

        case SupportedDatabase.SQLITE:
            sqlite_conn: sqlite3.Connection = sqlite3.connect(":memory:")
            sqlite_conn.isolation_level = "DEFERRED"
            try:
                yield sqlite_conn
            finally:
                sqlite_conn.close()

        case _:
            raise ValueError(f"Unsupported database type: {database_type}")


@pytest.fixture(
    params=[
        pytest.param(SupportedDatabase.POSTGRES, marks=pytest.mark.postgres, id="postgres"),
        pytest.param(SupportedDatabase.MYSQL, marks=pytest.mark.mysql, id="mysql"),
        pytest.param(SupportedDatabase.SQLITE, marks=pytest.mark.sqlite, id="sqlite"),
    ]
)
def client(request: pytest.FixtureRequest, temp_dir: Path) -> Generator[SqlClient[Any]]:
    """Parameterized fixture that yields a fully initialized client.

    Runs cmd_init(), loads the changelog, and returns a fresh client
    with the loaded changelog. The migrations table is created automatically.
    """
    database_type = request.param
    # Shared setup
    migrations_dir = temp_dir / "migrations"
    migrations_file = temp_dir / "changelog.json"
    with connection(database_type) as conn:
        if database_type is SupportedDatabase.POSTGRES:
            from migrateit.clients.psql import PsqlClient

            # Create migrations table
            sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
            with conn.cursor() as cursor:
                cursor.execute(sql)  # pyright: ignore
            conn.commit()
        elif database_type is SupportedDatabase.MYSQL:
            from migrateit.clients.mysql import MySqlClient

            # Create migrations table
            sql, _ = MySqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
            with conn.cursor() as cursor:
                cursor.execute(sql)  # pyright: ignore
            conn.commit()
        elif database_type is SupportedDatabase.SQLITE:
            # Create the migrations table
            sql, _ = SqliteClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
            conn.executescript(sql)
            conn.commit()
        else:
            raise ValueError(f"Unsupported database type: {database_type}")

        # Run cmd_init to create the initial migration (includes changelog creation)
        cmd_init(
            table_name=TEST_MIGRATIONS_TABLE,
            migrations_dir=migrations_dir,
            migrations_file=migrations_file,
            database=database_type,
        )

        # Reload changelog and create client
        changelog = load_changelog_file(migrations_file)
        config = MigrateItConfig(
            table_name=TEST_MIGRATIONS_TABLE,
            migrations_dir=migrations_dir,
            changelog=changelog,
        )
        yield get_client(config, conn)


def _create_migration_file(
    migrations_dir: Path,
    filename: str,
    sql: str = "SELECT 1;",
    rollback_sql: str = "SELECT 2;",
) -> Path:
    """Create a migration file with the standard rollback split tag."""
    migrations_dir.mkdir(parents=True, exist_ok=True)
    path = migrations_dir / filename
    with open(path, "w") as f:
        f.write(sql or f"-- Migration {filename}\n")
        f.write(f"{ROLLBACK_SPLIT_TAG}")
        f.write(f"\n\n{rollback_sql}")
    return path


def _get_query_rows(client: SqlClient[Any], query: str) -> list[tuple[Any, ...]]:
    """Execute a query and return rows, handling cursor vs direct execution."""
    match client.changelog.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL:
            with client.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        case SupportedDatabase.SQLITE:
            return client.connection.execute(query).fetchall()
        case _:
            raise NotImplementedError


def _migration_is_applied(client: SqlClient[Any], migration_name: str) -> bool:
    match client.changelog.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s",
                    (migration_name,),
                )
                result = cursor.fetchone()
                assert result is not None
                return bool(result[0])
        case SupportedDatabase.SQLITE:
            cursor = client.connection.execute(
                f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = ?",
                (migration_name,),
            )
            result = cursor.fetchone()
            assert result is not None
            return bool(result[0])
        case _:
            raise NotImplementedError


def _table_exists(client: SqlClient[Any], table_name: str) -> bool:
    match client.changelog.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
                    (TEST_TABLE,),
                )
                result = cursor.fetchone()
                assert result is not None
                return result[0]
        case SupportedDatabase.SQLITE:
            cursor = client.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            result = cursor.fetchone()
            return result is not None
        case _:
            raise NotImplementedError


def _drop_test_table(client: SqlClient[Any], table_name: str = TEST_TABLE) -> None:
    match client.changelog.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL:
            with client.connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        case SupportedDatabase.SQLITE:
            client.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        case _:
            raise NotImplementedError
    client.connection.commit()
