import os
import sqlite3
from pathlib import Path

import pytest

from migrateit.clients.sqlite import SqliteClient
from migrateit.models import Migration
from migrateit.models.changelog import ChangelogFile
from migrateit.models.migration import MigrationStatus
from tests.conftest import INIT_MIGRATION, TEST_MIGRATIONS_TABLE, create_migration_file


@pytest.mark.sqlite
def test_validate_valid_sql(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0001_init.sql"
    create_migration_file(migrations_dir, filename, sql="SELECT * FROM non_existing_table;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert sqlite_client.validate_sql_syntax(migration) is None


@pytest.mark.sqlite
def test_validate_create_table_syntax(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0002_create_table.sql"
    create_migration_file(
        migrations_dir,
        filename,
        sql="""
            CREATE TABLE non_existing_table (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert sqlite_client.validate_sql_syntax(migration) is None
    with pytest.raises(sqlite3.Error):
        sqlite_client.connection.execute("SELECT * FROM non_existing_table;")


@pytest.mark.sqlite
def test_validate_invalid_sql(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test SQL syntax validation with invalid SQL."""
    create_migration_file(
        temp_dir / "migrations",
        "0002_invalid.sql",
        sql="CRAete TABLE t (id INTEGER);",
        rollback_sql="DROP TABLE t;",
    )

    sqlite_client.changelog.migrations.append(
        Migration(name="0002_invalid.sql", initial=False, parents=["0000_migrateit.sql"])
    )
    result = sqlite_client.validate_sql_syntax(sqlite_client.changelog.migrations[1])
    assert result is not None
    assert isinstance(result[0], SyntaxError)


@pytest.mark.sqlite
def test_validate_drop_table_statement(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0008_drop_table.sql"
    create_migration_file(migrations_dir, filename, sql="DROP TABLE non_existing_table;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert sqlite_client.validate_sql_syntax(migration) is None


# --- validate_migrations conflict path ---


@pytest.mark.sqlite
def test_validate_conflict_raises(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    filename = "0001_test.sql"
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    create_migration_file(migrations_dir, filename)

    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name=filename, parents=["0000_init.sql"]),
    ]
    sqlite_client.config.changelog = ChangelogFile(version=1, migrations=migrations)

    # Insert a different hash into the DB to trigger conflict
    sqlite_client.connection.execute(
        f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (?, ?)",
        (filename, "different_hash"),
    )
    sqlite_client.connection.commit()

    statuses = sqlite_client.retrieve_migration_statuses()
    assert statuses[filename] == MigrationStatus.CONFLICT
    with pytest.raises(ValueError, match="has a different hash"):
        sqlite_client.validate_migrations(statuses)
