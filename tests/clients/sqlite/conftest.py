import sqlite3
from collections.abc import Generator
from pathlib import Path

import pytest

from migrateit.clients.sqlite import SqliteClient
from migrateit.models import MigrateItConfig
from migrateit.models.changelog import SupportedDatabase
from migrateit.tree import create_changelog_file, create_new_migration
from tests.conftest import TEST_MIGRATIONS_TABLE


@pytest.fixture()
def sqlite_conn() -> Generator[sqlite3.Connection]:
    """Test-scoped in-memory SQLite connection.

    SQLite in-memory databases are per-connection — they vanish when the
    connection closes and can't be shared across tests. Each test gets its
    own fresh database for complete isolation. No cleanup needed.
    """
    conn = sqlite3.connect(":memory:")
    conn.isolation_level = "DEFERRED"
    yield conn
    conn.close()


@pytest.fixture()
def sqlite_client(sqlite_conn: sqlite3.Connection, temp_dir: Path) -> Generator[SqliteClient]:
    """Create a SqliteClient with a clean test table and initial migration."""
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    changelog = create_changelog_file(temp_dir / "changelog.json", SupportedDatabase.SQLITE)

    create_new_migration(changelog=changelog, migrations_dir=migrations_dir, name="migrateit")
    config = MigrateItConfig(
        table_name=TEST_MIGRATIONS_TABLE,
        migrations_dir=migrations_dir,
        changelog=changelog,
    )
    client = SqliteClient(connection=sqlite_conn, config=config)
    sql, _ = SqliteClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    sqlite_conn.executescript(sql)
    yield client
