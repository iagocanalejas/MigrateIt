import os
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import psycopg
import pytest

from migrateit.cli import cmd_init
from migrateit.clients import PsqlClient
from migrateit.models import MigrateItConfig
from migrateit.models.changelog import ChangelogFile, Migration, SupportedDatabase
from migrateit.tree import ROLLBACK_SPLIT_TAG, create_changelog_file, load_changelog_file

INIT_MIGRATION = "0000_migrateit.sql"
TEST_MIGRATIONS_TABLE = "migrations"


@pytest.fixture(autouse=True)
def suppress_write_line_b() -> Generator[None]:
    with patch("migrateit.reporters.output.write_line_b", lambda *_: None):
        yield


@pytest.fixture(autouse=True)
def suppress_write_line() -> Generator[None]:
    with patch("migrateit.reporters.output.write_line", lambda *_: None):
        yield


@pytest.fixture(scope="session")
def _postgres_connection() -> Generator[psycopg.Connection]:
    """Session-scoped PostgreSQL connection for integration tests."""
    conn = psycopg.connect(PsqlClient.get_environment_url())
    yield conn
    conn.close()


@pytest.fixture()
def pg_conn(_postgres_connection: psycopg.Connection) -> psycopg.Connection:
    """Expose the PostgreSQL connection for tests that need cursor operations."""
    return _postgres_connection


@pytest.fixture()
def temp_dir() -> Generator[Path]:
    """Create a temporary directory, cleaned up after the test."""
    d = Path(tempfile.mkdtemp())
    yield d
    shutil.rmtree(d)


def _drop_table(conn: psycopg.Connection, table: str = TEST_MIGRATIONS_TABLE) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")  # pyright: ignore
    conn.commit()


def _ensure_table_created(conn: psycopg.Connection, table: str = TEST_MIGRATIONS_TABLE) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(table)
    with conn.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    conn.commit()


@pytest.fixture()
def client(
    _postgres_connection: psycopg.Connection,
    temp_dir: Path,
) -> Generator[PsqlClient]:
    """Create a PsqlClient with a clean test table for psql tests."""
    migrations_dir = temp_dir / "migrations"
    changelog = create_changelog_file(temp_dir / "changelog.json", SupportedDatabase.POSTGRES)
    config = MigrateItConfig(
        table_name=TEST_MIGRATIONS_TABLE,
        migrations_dir=migrations_dir,
        changelog=changelog,
    )
    psql_client = PsqlClient(connection=_postgres_connection, config=config)
    _ensure_table_created(_postgres_connection)

    # NOTE: do NOT delete temp_dir here — the temp_dir fixture handles cleanup.
    yield psql_client

    _drop_table(_postgres_connection)


@pytest.fixture()
def cmd_client(
    _postgres_connection: psycopg.Connection,
    temp_dir: Path,
) -> Generator[PsqlClient]:
    """Create a PsqlClient with a clean test table for cmd tests.

    Note: does NOT create the changelog file — tests call cmd_init() to create it.
    """
    migrations_dir = temp_dir / "migrations"
    # Create a placeholder changelog path; cmd_init will create the real one
    changelog = ChangelogFile(version=1, migrations=[], path=temp_dir / "changelog.json")
    config = MigrateItConfig(
        table_name=TEST_MIGRATIONS_TABLE,
        migrations_dir=migrations_dir,
        changelog=changelog,
    )
    psql_client = PsqlClient(connection=_postgres_connection, config=config)
    _ensure_table_created(_postgres_connection)

    # NOTE: do NOT delete temp_dir here — the temp_dir fixture handles cleanup.
    yield psql_client

    _drop_table(_postgres_connection)


def create_migration_file(
    migrations_dir: Path,
    filename: str,
    sql: str | None = None,
    rollback_sql: str | None = None,
) -> str:
    path = os.path.join(migrations_dir, filename)
    with open(path, "w") as f:
        f.write(sql or f"-- Migration {filename}\n")
        f.write(f"{ROLLBACK_SPLIT_TAG}")
        if rollback_sql:
            f.write(f"\n\n{rollback_sql}")
    return path


def create_empty_changelog(temp_dir: Path) -> ChangelogFile:
    return ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION)], path=temp_dir / "changelog.json")


def setup_test_client(
    pg_conn: psycopg.Connection,
    cmd_client: PsqlClient,
) -> PsqlClient:
    """Set up a fully initialized PsqlClient for integration tests.

    Runs cmd_init(), loads the changelog, and returns a fresh client
    with the loaded changelog.
    """

    cmd_init(
        table_name="migrations",
        migrations_dir=cmd_client.migrations_dir,
        migrations_file=cmd_client.changelog.path,
        database=SupportedDatabase.POSTGRES,
    )

    changelog = load_changelog_file(cmd_client.changelog.path)
    config = MigrateItConfig(
        table_name="migrations",
        migrations_dir=cmd_client.migrations_dir,
        changelog=changelog,
    )
    return PsqlClient(connection=pg_conn, config=config)
