import sqlite3
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from migrateit.cli import cmd_init
from migrateit.clients import SqlClient
from migrateit.clients.sqlite import SqliteClient
from migrateit.models import MigrateItConfig
from migrateit.models.changelog import SupportedDatabase
from migrateit.tree import load_changelog_file
from tests.conftest import TEST_MIGRATIONS_TABLE


@pytest.fixture(params=["psql", "sqlite"], ids=["psql", "sqlite"])
def cmd_client(request: pytest.FixtureRequest, temp_dir: Path) -> Generator[SqlClient[Any]]:
    """Parameterized fixture that yields a fully initialized client for both PostgreSQL and SQLite.

    Runs cmd_init(), loads the changelog, and returns a fresh client
    with the loaded changelog. The migrations table is created automatically.
    """
    database_type: str = request.param

    # Shared setup
    migrations_dir = temp_dir / "migrations"
    migrations_file = temp_dir / "changelog.json"

    if database_type == "psql":
        import psycopg

        from migrateit.clients.psql import PsqlClient

        # Get session-scoped connection
        conn: psycopg.Connection = psycopg.connect(PsqlClient.get_environment_url())

        # Create migrations table
        sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
        with conn.cursor() as cursor:
            cursor.execute(sql)  # pyright: ignore
        conn.commit()

        # Run cmd_init to create the initial migration (includes changelog creation)
        cmd_init(
            table_name=TEST_MIGRATIONS_TABLE,
            migrations_dir=migrations_dir,
            migrations_file=migrations_file,
            database=SupportedDatabase.POSTGRES,
        )

        # Reload changelog and create client
        changelog = load_changelog_file(migrations_file)
        config = MigrateItConfig(
            table_name=TEST_MIGRATIONS_TABLE,
            migrations_dir=migrations_dir,
            changelog=changelog,
        )
        client: SqlClient[Any] = PsqlClient(connection=conn, config=config)

        yield client

        # Cleanup
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
        conn.commit()
        conn.close()

    else:
        # SQLite: in-memory connection
        cmd_init(
            table_name=TEST_MIGRATIONS_TABLE,
            migrations_dir=migrations_dir,
            migrations_file=migrations_file,
            database=SupportedDatabase.SQLITE,
        )

        # Load changelog and create client
        changelog = load_changelog_file(migrations_file)
        config = MigrateItConfig(
            table_name=TEST_MIGRATIONS_TABLE,
            migrations_dir=migrations_dir,
            changelog=changelog,
        )
        sqlite_conn: sqlite3.Connection = sqlite3.connect(":memory:")
        sqlite_conn.isolation_level = "DEFERRED"
        client = SqliteClient(connection=sqlite_conn, config=config)

        # Create the migrations table
        sql, _ = SqliteClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
        sqlite_conn.executescript(sql)

        yield client

        # Cleanup
        sqlite_conn.close()


def get_query_rows(conn: Any, query: str) -> list[tuple[Any, ...]]:
    """Execute a query and return rows, handling cursor vs direct execution."""
    if isinstance(conn, sqlite3.Connection):
        # SQLite: execute directly on connection
        return conn.execute(query).fetchall()
    else:
        # PostgreSQL: use cursor context manager
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()
