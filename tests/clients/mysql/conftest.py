from collections.abc import Generator
from pathlib import Path

import pytest
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection

from migrateit.clients.mysql import MySqlClient
from migrateit.models import MigrateItConfig
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.connection import get_connection
from migrateit.tree import create_changelog_file, create_new_migration
from tests.conftest import TEST_MIGRATIONS_TABLE

type _MySqlConnection = MySQLConnectionAbstract | PooledMySQLConnection


@pytest.fixture(scope="session")
def _mysql_connection() -> Generator[_MySqlConnection]:
    """Session-scoped PostgreSQL connection.

    PostgreSQL connections are expensive (network round-trip, authentication,
    server-side resources), so we reuse a single connection across all tests.
    Tests share the same DB state and clean up after themselves with
    DROP TABLE IF EXISTS. Use a private name (_postgres_connection) to signal
    it's an internal fixture consumed by other fixtures; expose via pg_conn.
    """
    conn = get_connection(SupportedDatabase.MYSQL)
    assert isinstance(conn, MySQLConnectionAbstract | PooledMySQLConnection)
    yield conn
    conn.close()


@pytest.fixture()
def mysql_conn(_mysql_connection: _MySqlConnection) -> _MySqlConnection:  # pragma: no cover
    """Expose the PostgreSQL connection for tests that need cursor operations."""
    return _mysql_connection


@pytest.fixture()
def mysql_client(_mysql_connection: _MySqlConnection, temp_dir: Path) -> Generator[MySqlClient]:
    """Create a MySqlClient with a clean test table and initial migration."""
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    changelog = create_changelog_file(temp_dir / "changelog.json", SupportedDatabase.MYSQL)

    create_new_migration(changelog=changelog, migrations_dir=migrations_dir, name="migrateit")
    config = MigrateItConfig(
        table_name=TEST_MIGRATIONS_TABLE,
        migrations_dir=migrations_dir,
        changelog=changelog,
    )
    mysql_client = MySqlClient(connection=_mysql_connection, config=config)
    sql, _ = MySqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with _mysql_connection.cursor() as cursor:
        cursor.execute(sql)
    _mysql_connection.commit()

    yield mysql_client

    # we need clean database between tests
    with _mysql_connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
    _mysql_connection.commit()
