from pathlib import Path

import pytest

from migrateit.clients.sqlite import SqliteClient
from migrateit.models import Migration
from tests.conftest import create_migration_file


@pytest.mark.sqlite
def test_squash_migrations(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test squashing migrations."""
    migrations = [
        ("0001_first.sql", "SELECT 1;", "SELECT 1;"),
        ("0002_second.sql", "SELECT 2;", "SELECT 2;"),
    ]
    for name, sql, rollback in migrations:
        create_migration_file(temp_dir / "migrations", name, sql=sql, rollback_sql=rollback)
        sqlite_client.changelog.migrations.append(Migration(name=name, initial=False, parents=["0000_migrateit.sql"]))

    for m in sqlite_client.changelog.migrations[1:]:
        sqlite_client.apply_migration(m)
    sqlite_client.connection.commit()

    new_migration = Migration(name="0003_squashed_0001_0002.sql", initial=False, parents=["0000_migrateit.sql"])
    sqlite_client.squash_migrations(["0001_first.sql", "0002_second.sql"], new_migration)
    sqlite_client.connection.commit()

    cursor = sqlite_client.connection.execute(f"SELECT migration_name, squashed FROM {sqlite_client.table_name}")
    rows = {row[0]: row[1] for row in cursor.fetchall()}
    assert rows.get("0001_first.sql") == 1
    assert rows.get("0002_second.sql") == 1
    assert "0003_squashed_0001_0002.sql" in rows
