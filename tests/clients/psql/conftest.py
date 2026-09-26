from collections.abc import Generator
from pathlib import Path

import psycopg
import pytest

from migrateit.clients import PsqlClient
from migrateit.models import MigrateItConfig
from migrateit.models.changelog import SupportedDatabase
from migrateit.tree import create_changelog_file, create_new_migration
from tests.conftest import TEST_MIGRATIONS_TABLE


@pytest.fixture(scope="session")
def _postgres_connection() -> Generator[psycopg.Connection]:
    """Session-scoped PostgreSQL connection.

    PostgreSQL connections are expensive (network round-trip, authentication,
    server-side resources), so we reuse a single connection across all tests.
    Tests share the same DB state and clean up after themselves with
    DROP TABLE IF EXISTS. Use a private name (_postgres_connection) to signal
    it's an internal fixture consumed by other fixtures; expose via pg_conn.
    """
    conn = psycopg.connect(PsqlClient.get_environment_url())
    yield conn
    conn.close()


@pytest.fixture()
def pg_conn(_postgres_connection: psycopg.Connection) -> psycopg.Connection:  # pragma: no cover
    """Expose the PostgreSQL connection for tests that need cursor operations."""
    return _postgres_connection


@pytest.fixture()
def pg_client(_postgres_connection: psycopg.Connection, temp_dir: Path) -> Generator[PsqlClient]:
    """Create a PsqlClient with a clean test table for PostgreSQL tests."""
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    changelog = create_changelog_file(temp_dir / "changelog.json", SupportedDatabase.POSTGRES)

    create_new_migration(changelog=changelog, migrations_dir=migrations_dir, name="migrateit")
    config = MigrateItConfig(
        table_name=TEST_MIGRATIONS_TABLE,
        migrations_dir=migrations_dir,
        changelog=changelog,
    )
    psql_client = PsqlClient(connection=_postgres_connection, config=config)
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with _postgres_connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    _postgres_connection.commit()

    yield psql_client

    # we need clean database between tests
    with _postgres_connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")  # pyright: ignore
    _postgres_connection.commit()
