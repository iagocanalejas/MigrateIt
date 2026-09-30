import os
from pathlib import Path

import pytest

from migrateit.clients.mysql import MySqlClient
from migrateit.models import ChangelogFile, Migration
from migrateit.models.migration import MigrationStatus
from tests.conftest import INIT_MIGRATION, TEST_MIGRATIONS_TABLE, create_migration_file


@pytest.mark.mysql
def test_validate_valid_sql(mysql_client: MySqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0001_init.sql"
    create_migration_file(migrations_dir, filename, sql="SELECT * FROM non_existing_table;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert mysql_client.validate_sql_syntax(migration) is None


@pytest.mark.mysql
def test_validate_invalid_sql(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test SQL syntax validation with invalid SQL."""
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0003_invalid.sql"
    create_migration_file(migrations_dir, filename, sql="SELEKT * FRM non_existing_table;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])

    error_result = mysql_client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, SyntaxError)
    assert "SELEKT" in sql


@pytest.mark.mysql
def test_validate_conflict_raises(mysql_client: MySqlClient, temp_dir: Path) -> None:
    filename = "0001_test.sql"
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    create_migration_file(migrations_dir, filename)

    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name=filename, parents=["0000_init.sql"]),
    ]
    mysql_client.config.changelog = ChangelogFile(version=1, migrations=migrations)

    # Insert a different hash into the DB to trigger conflict
    with mysql_client.connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (%s, %s)",
            (filename, "different_hash"),
        )
    mysql_client.connection.commit()

    statuses = mysql_client.retrieve_migration_statuses()
    assert statuses[filename] == MigrationStatus.CONFLICT
    with pytest.raises(ValueError, match="has a different hash"):
        mysql_client.validate_migrations(statuses)


@pytest.mark.mysql
def test_validate_migrations_empty_changelog(mysql_client: MySqlClient) -> None:
    """Test validate_migrations early returns when changelog is empty."""
    mysql_client.changelog.migrations = []
    mysql_client.validate_migrations({})


@pytest.mark.mysql
def test_validate_migrations_missing_initial(mysql_client: MySqlClient) -> None:
    """Test validate_migrations raises when initial migration is missing."""
    mysql_client.changelog.migrations = [
        Migration(name="0001_second.sql", parents=["0000_migrateit.sql"]),
    ]
    with pytest.raises(ValueError, match="Initial migration is not defined"):
        mysql_client.validate_migrations({})


@pytest.mark.mysql
def test_validate_migrations_multiple_initial(mysql_client: MySqlClient) -> None:
    """Test validate_migrations raises when multiple initial migrations exist."""
    mysql_client.changelog.migrations = [
        Migration(name="0000_first.sql", initial=True, parents=[]),
        Migration(name="0000_second.sql", initial=True, parents=[]),
    ]
    with pytest.raises(ValueError, match="Multiple initial migrations"):
        mysql_client.validate_migrations({})


@pytest.mark.mysql
def test_validate_removed_migrations(mysql_client: MySqlClient, temp_dir: Path) -> None:
    migrations = [Migration(name="0000_init.sql", initial=True, parents=[])]
    mysql_client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = {"0000_init.sql": MigrationStatus.APPLIED, "ghost.sql": MigrationStatus.REMOVED}
    with pytest.raises(ValueError, match="Removed migrations found"):
        mysql_client.validate_migrations(statuses)
