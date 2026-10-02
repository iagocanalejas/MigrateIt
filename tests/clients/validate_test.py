from pathlib import Path
from typing import Any

import pytest

from migrateit.clients._client import SqlClient
from migrateit.models.changelog import ChangelogFile, SupportedDatabase
from migrateit.models.migration import Migration, MigrationStatus
from tests.clients._clients_test import MIGRATION_NAME
from tests.conftest import INITIAL_MIGRATION, TEST_MIGRATIONS_TABLE, _create_migration_file, _table_exists


def _insert_migration_hash(client: SqlClient[Any], name: str, hash_value: str) -> None:
    match client.changelog.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (%s, %s)",
                    (name, hash_value),
                )
        case SupportedDatabase.SQLITE:
            client.connection.execute(
                f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (?, ?)",
                (name, hash_value),
            )
        case _:
            raise NotImplementedError
    client.connection.commit()


# --- validate_sql_syntax ---


def test_validate_simple_select_syntax(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(migrations_dir, MIGRATION_NAME, sql="SELECT * FROM non_existing_table;")
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])
    assert client.validate_sql_syntax(migration) is None


def test_validate_create_table_syntax(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql="""
            CREATE TABLE non_existing_table (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])

    assert client.validate_sql_syntax(migration) is None
    assert not _table_exists(client, "non_existing_table")


def test_invalid_sql_in_migration_code(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(migrations_dir, MIGRATION_NAME, sql="SELEKT * FRM non_existing_table;")
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])

    error_result = client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, SyntaxError)
    assert "SELEKT" in sql


def test_invalid_sql_in_rollback_code(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(migrations_dir, MIGRATION_NAME, rollback_sql="ROLLBAK;")
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])

    error_result = client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, SyntaxError)
    assert "ROLLBAK" in sql


def test_empty_sql_file_is_skipped(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(migrations_dir, MIGRATION_NAME, sql="", rollback_sql="")
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])
    assert client.validate_sql_syntax(migration) is None


def test_file_not_found_raises_error(client: SqlClient[Any]) -> None:
    migration = Migration(name="not_exist.sql", parents=[INITIAL_MIGRATION])
    with pytest.raises(FileNotFoundError):
        client.validate_sql_syntax(migration)


def test_non_sql_file_raises_error(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"

    filename = "0006_script.txt"
    path = migrations_dir / filename
    path.write_text("SELECT 1;")
    migration = Migration(name=filename, parents=[INITIAL_MIGRATION])
    with pytest.raises(FileNotFoundError):
        client.validate_sql_syntax(migration)


def test_validate_multiple_statements(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql=f"""
            INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES ('1', 'hash1');
            INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES ('2', 'hash2');
        """,
    )
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])

    assert client.validate_sql_syntax(migration) is None


def test_validate_drop_table_statement(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(migrations_dir, MIGRATION_NAME, sql="DROP TABLE non_existing_table;")
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])

    assert client.validate_sql_syntax(migration) is None


def test_validate_alter_table_add_column(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql=f"ALTER TABLE {TEST_MIGRATIONS_TABLE} ADD COLUMN new_col TEXT;",
    )
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])
    assert client.validate_sql_syntax(migration) is None


def test_validate_alter_table_drop_column(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql=f"ALTER TABLE {TEST_MIGRATIONS_TABLE} DROP COLUMN to_remove;",
    )
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])
    assert client.validate_sql_syntax(migration) is None


def test_invalid_drop_table_statement(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(migrations_dir, MIGRATION_NAME, sql="DROP TABL test_table;")
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])

    error_result = client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, SyntaxError)
    assert "DROP TABL" in sql


def test_invalid_alter_table_statement(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(migrations_dir, MIGRATION_NAME, sql="ALTER TABLE some_table ADD COLUM typo_col TEXT;")
    migration = Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION])

    error_result = client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, SyntaxError)
    assert "ADD COLUM" in sql


# --- validate_migrations ---


def test_validate_migrations_success(client: SqlClient[Any]) -> None:
    migrations = [
        Migration(name=INITIAL_MIGRATION, initial=True, parents=[]),
        Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION]),
    ]
    client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    status_map = {
        INITIAL_MIGRATION: MigrationStatus.APPLIED,
        MIGRATION_NAME: MigrationStatus.APPLIED,
    }
    client.validate_migrations(status_map)


def test_validate_empty_migrations(client: SqlClient[Any]) -> None:
    client.config.changelog = ChangelogFile(version=1, migrations=[])
    statuses: dict[str, MigrationStatus] = {}
    client.validate_migrations(statuses)


def test_validate_no_initial_raises(client: SqlClient[Any]) -> None:
    migrations = [Migration(name="0001_test.sql", parents=[])]
    client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses: dict[str, MigrationStatus] = {}
    with pytest.raises(ValueError, match="Initial migration is not defined"):
        client.validate_migrations(statuses)


def test_validate_multiple_initial_raises(client: SqlClient[Any]) -> None:
    migrations = [
        Migration(name="0000_a.sql", initial=True, parents=[]),
        Migration(name="0001_b.sql", initial=True, parents=[]),
    ]
    client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses: dict[str, MigrationStatus] = {}
    with pytest.raises(ValueError, match="Multiple initial migrations found"):
        client.validate_migrations(statuses)


def test_validate_removed_raises(client: SqlClient[Any]) -> None:
    migrations = [Migration(name="0000_init.sql", initial=True, parents=[])]
    client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = {"0000_init.sql": MigrationStatus.APPLIED, "ghost.sql": MigrationStatus.REMOVED}
    with pytest.raises(ValueError, match="Removed migrations found"):
        client.validate_migrations(statuses)


def test_validate_parent_not_applied(client: SqlClient[Any]) -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_child.sql", parents=["0000_init.sql"]),
    ]
    client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = {
        "0000_init.sql": MigrationStatus.NOT_APPLIED,
        "0001_child.sql": MigrationStatus.APPLIED,
    }
    with pytest.raises(ValueError, match="applied before"):
        client.validate_migrations(statuses)


def test_validate_conflict_raises(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    _create_migration_file(migrations_dir, MIGRATION_NAME)

    client.changelog.migrations = [
        Migration(name=INITIAL_MIGRATION, initial=True, parents=[]),
        Migration(name=MIGRATION_NAME, parents=[INITIAL_MIGRATION]),
    ]

    # Insert a different hash into the DB to trigger conflict
    _insert_migration_hash(client, MIGRATION_NAME, "different_hash")

    statuses = client.retrieve_migration_statuses()
    assert statuses[MIGRATION_NAME] == MigrationStatus.CONFLICT
    with pytest.raises(ValueError, match="has a different hash"):
        client.validate_migrations(statuses)


def test_show_migrations_order_error(client: SqlClient[Any]) -> None:
    """Test validate_migrations raises when child is applied before parent."""
    changelog = ChangelogFile(
        version=1,
        migrations=[
            Migration(name="0000_migrateit.sql", initial=True, parents=[]),
            Migration(name="0001_first.sql", parents=["0000_migrateit.sql"]),
            Migration(name="0002_second.sql", parents=["0001_first.sql"]),
        ],
    )
    client.config.changelog = changelog

    statuses = {
        "0000_migrateit.sql": MigrationStatus.APPLIED,
        "0001_first.sql": MigrationStatus.NOT_APPLIED,
        "0002_second.sql": MigrationStatus.APPLIED,
    }
    with pytest.raises(ValueError, match="is applied before its parent"):
        client.validate_migrations(statuses)
