import functools
import os
import sqlite3
import tempfile
from collections.abc import Callable, Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from migrateit import constants as C
from migrateit.clients._client import SqlClient, get_client
from migrateit.clients.mysql import MySqlClient
from migrateit.cmd import cmd_init, cmd_run
from migrateit.models.config import MigrateItConfig, SupportedDatabase
from migrateit.models.repository import ChangelogRepository

INITIAL_MIGRATION = "0000_migrateit.sql"
MIGRATION_NAME = "0001_test_table.sql"
TEST_MIGRATIONS_TABLE = "migrations"
TEST_TABLE = "test_entity"


def _get_mysql_port(database: SupportedDatabase) -> int:
    if database is SupportedDatabase.MARIADB:
        return int(os.getenv("DB_PORT_MARIADB", "3306"))
    return int(os.getenv("DB_PORT_MYSQL", "3317"))


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session: pytest.Session) -> None:
    print("\n\033[1;33m⚠️  WARNING: @functools.cached_property is globally DISABLED for tests.\n\033[0m")

    # Globally patch cached_property.__get__ for the whole session
    original_get = functools.cached_property.__get__

    def bypass_cache_get[T](self: Any, instance: Callable[[Any], T] | None, owner: Any = None) -> T:
        if instance is None:
            return self
        return self.func(instance)

    functools.cached_property.__get__ = bypass_cache_get  # type: ignore

    # Store original_get on the session so we can restore it if needed
    # (though pytest teardown isn't strictly necessary for session-end globals)
    session._original_cached_property_get = original_get  # type: ignore


@pytest.fixture(autouse=True, scope="session")
def suppress_write() -> Generator[None]:
    with (
        patch("migrateit.reporters.output.write_line_b", lambda *_: None),
        patch("migrateit.reporters.output.write_line", lambda *_: None),
    ):
        yield


@pytest.fixture()
def temp_dir() -> Generator[Path]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@contextmanager
def connection(database_type: SupportedDatabase) -> Generator[Any]:
    match database_type:
        case SupportedDatabase.POSTGRES:
            import psycopg

            from migrateit.clients.psql import PsqlClient

            pg_conn: psycopg.Connection = psycopg.connect(**PsqlClient.get_connection_params())
            try:
                yield pg_conn
            finally:
                try:
                    pg_conn.rollback()
                except Exception:  # pragma: no cover[safety]
                    pass

                with pg_conn.cursor() as cursor:
                    cursor.execute("DROP SCHEMA public CASCADE;")
                    cursor.execute("DROP SCHEMA IF EXISTS app_schema CASCADE;")
                    cursor.execute("CREATE SCHEMA public;")
                pg_conn.commit()
                pg_conn.close()

        case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            import mysql.connector

            # Override the host port so the fixture can target MySQL (3317) or
            # MariaDB (3306) running in docker-compose independently.
            params = MySqlClient.get_connection_params()
            params["port"] = _get_mysql_port(database_type)
            db_name = params["database"].lstrip("/")
            mysql_conn = mysql.connector.connect(**params)
            try:
                yield mysql_conn
            finally:
                try:
                    mysql_conn.rollback()
                except Exception:  # pragma: no cover[safety]
                    pass

                with mysql_conn.cursor() as cursor:
                    cursor.execute(f"DROP DATABASE IF EXISTS `{db_name}`;")
                    cursor.execute(f"CREATE DATABASE `{db_name}`;")
                mysql_conn.commit()
                mysql_conn.close()

        case SupportedDatabase.ORACLE:
            import oracledb

            from migrateit.clients.oracle import OracleClient

            params = OracleClient.get_connection_params()
            conn_kwargs = {
                "user": params["user"],
                "password": params["password"],
                "host": params.get("host", "localhost"),
                "port": params.get("port", 1521),
                "service_name": params.get("service_name"),
                "sid": params.get("sid"),
            }
            oracle_conn = oracledb.connect(**conn_kwargs)
            try:
                yield oracle_conn
            finally:
                try:
                    oracle_conn.rollback()
                except Exception:  # pragma: no cover[safety]
                    pass
                with oracle_conn.cursor() as cursor:
                    cursor.execute("""
BEGIN
    -- 1. Drop Views
    FOR v IN (SELECT view_name FROM user_views) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP VIEW "' || v.view_name || '"';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    -- 2. Drop Materialized Views
    FOR mv IN (SELECT mview_name FROM user_mviews) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW "' || mv.mview_name || '"';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    -- 3. Drop Packages, Procedures, Functions, Triggers, and Synonyms
    FOR o IN (SELECT object_name, object_type FROM user_objects
              WHERE object_type IN ('PACKAGE', 'PROCEDURE', 'FUNCTION', 'TRIGGER', 'SYNONYM')) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP ' || o.object_type || ' "' || o.object_name || '"';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    -- 4. Drop Types (using FORCE to handle type dependencies)
    FOR tp IN (SELECT type_name FROM user_types) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP TYPE "' || tp.type_name || '" FORCE';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    -- 5. Drop Tables (with CASCADE CONSTRAINTS)
    FOR t IN (SELECT table_name FROM user_tables) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE "' || t.table_name || '" CASCADE CONSTRAINTS';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    -- 6. Drop Standalone Sequences
    FOR s IN (SELECT sequence_name FROM user_sequences
              WHERE sequence_name NOT IN (SELECT NVL(sequence_name, 'DUMMY') FROM user_tab_identity_cols)) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP SEQUENCE "' || s.sequence_name || '"';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
END;
                    """)
                    # Purge recycle bin to prevent ORA-00955 naming conflicts
                    cursor.execute("PURGE RECYCLEBIN")
                oracle_conn.commit()
                oracle_conn.close()

        case SupportedDatabase.SQLITE:
            sqlite_conn: sqlite3.Connection = sqlite3.connect(":memory:", autocommit=False)
            try:
                yield sqlite_conn
            finally:
                try:
                    sqlite_conn.rollback()
                except Exception:  # pragma: no cover[safety]
                    pass
                sqlite_conn.close()

        case _:  # pragma: no cover
            raise ValueError(f"Unsupported database type: {database_type}")


@pytest.fixture(
    params=[
        pytest.param(SupportedDatabase.POSTGRES, marks=pytest.mark.postgres, id="postgres"),
        pytest.param(SupportedDatabase.MYSQL, marks=pytest.mark.mysql, id="mysql"),
        pytest.param(SupportedDatabase.MARIADB, marks=pytest.mark.mariadb, id="mariadb"),
        pytest.param(SupportedDatabase.SQLITE, marks=pytest.mark.sqlite, id="sqlite"),
        pytest.param(SupportedDatabase.ORACLE, marks=pytest.mark.oracle, id="oracle"),
    ]
)
def client(request: pytest.FixtureRequest, temp_dir: Path) -> Generator[SqlClient[Any]]:
    database_type = request.param
    # Shared setup
    migrations_dir = temp_dir / "migrations"
    migrations_file = temp_dir / "changelog.json"
    with connection(database_type) as conn:
        # Run cmd_init to create the initial migration (includes changelog creation)
        cmd_init(
            table_name=TEST_MIGRATIONS_TABLE,
            migrations_dir=migrations_dir,
            migrations_file=migrations_file,
            database=database_type,
        )

        # Reload changelog and create client
        changelog = ChangelogRepository.load(migrations_file)
        config = MigrateItConfig(
            table_name=TEST_MIGRATIONS_TABLE,
            migrations_dir=migrations_dir,
            changelog=changelog,
        )
        client = get_client(config, conn)

        # Run cmd_run to apply initial migration
        cmd_run(client=client)
        yield client


def _create_migration_file(
    migrations_dir: Path,
    filename: str,
    sql: str = "SELECT 1;",
    rollback_sql: str = "SELECT 2;",
) -> Path:
    migrations_dir.mkdir(parents=True, exist_ok=True)
    path = migrations_dir / filename
    with open(path, "w") as f:
        f.write(sql)
        f.write(f"{C.ROLLBACK_SPLIT_TAG}")
        f.write(f"\n\n{rollback_sql}")
    return path


def _get_query_rows(client: SqlClient[Any], query: str) -> list[tuple[Any, ...]]:
    match client.database:
        case (
            SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB | SupportedDatabase.ORACLE
        ):
            with client.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        case SupportedDatabase.SQLITE:
            return client.connection.execute(query).fetchall()
        case _:  # pragma: no cover
            raise NotImplementedError


def _migration_is_applied(client: SqlClient[Any], migration_name: str) -> bool:
    table_name = client._q(TEST_MIGRATIONS_TABLE)
    match client.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE migration_name = %s",
                    (migration_name,),
                )
                result = cursor.fetchone()
                assert result is not None
                return bool(result[0])
        case SupportedDatabase.ORACLE:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE migration_name = :1",
                    (migration_name,),
                )
                result = cursor.fetchone()
                assert result is not None
                return bool(result[0])
        case SupportedDatabase.SQLITE:
            cursor = client.connection.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE migration_name = ?",
                (migration_name,),
            )
            result = cursor.fetchone()
            assert result is not None
            return bool(result[0])
        case _:  # pragma: no cover
            raise NotImplementedError


def _table_exists(client: SqlClient[Any], table_name: str) -> bool:
    match client.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
                    (table_name,),
                )
                result = cursor.fetchone()
                assert result is not None
                return result[0]
        case SupportedDatabase.ORACLE:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM user_tables WHERE UPPER(table_name) = UPPER(:1)",
                    (table_name,),
                )
                result = cursor.fetchone()
                assert result is not None
                return bool(result[0])
        case SupportedDatabase.SQLITE:
            cursor = client.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            result = cursor.fetchone()
            return result is not None
        case _:  # pragma: no cover
            raise NotImplementedError


def _mock_create_migration(temp_dir: Path, database: SupportedDatabase) -> None:
    migrations_dir = temp_dir / "migrations"
    query = f"""CREATE TABLE IF NOT EXISTS {TEST_TABLE} (id SERIAL PRIMARY KEY);"""
    rollback = f"""DROP TABLE IF EXISTS {TEST_TABLE};"""
    if database == SupportedDatabase.ORACLE:
        query = f"""CREATE TABLE "{TEST_TABLE}" (id NUMBER PRIMARY KEY);"""
        rollback = f"""DROP TABLE "{TEST_TABLE}";"""
    _create_migration_file(migrations_dir, MIGRATION_NAME, sql=query, rollback_sql=rollback)


def _drop_test_table(client: SqlClient[Any], table_name: str) -> None:
    table_name = client._q(table_name)
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
