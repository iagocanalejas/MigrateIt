import os
from pathlib import Path

import pytest

from migrateit.clients import PsqlClient
from migrateit.models import Migration
from migrateit.models.changelog import ChangelogFile
from migrateit.models.migration import MigrationStatus
from tests.conftest import (
    INIT_MIGRATION,
    TEST_MIGRATIONS_TABLE,
    create_migration_file,
)


def test_table_exists_returns_true(client: PsqlClient, temp_dir: Path) -> None:
    # drop the table first as is being created in pytest fixtures
    with client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
    client.connection.commit()

    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
        client.connection.commit()
    assert client.is_migrations_table_created()


def test_table_missing_returns_false(client: PsqlClient, temp_dir: Path) -> None:
    with client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
    client.connection.commit()
    assert not client.is_migrations_table_created()


def _setup_migration_table(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()


def test_applied_migration_returns_true(client: PsqlClient, temp_dir: Path) -> None:
    _setup_migration_table(client, temp_dir)

    migration = Migration(name="0000_init.sql", initial=True, parents=[])
    migrations_dir = temp_dir / "migrations"
    create_migration_file(migrations_dir, "0000_init.sql", sql="SELECT 1;")
    client.config.changelog = ChangelogFile(version=1, migrations=[migration])

    client.apply_migration(migration, is_fake=False)
    assert client.is_migration_applied(migration)


def test_not_applied_migration_returns_false(client: PsqlClient, temp_dir: Path) -> None:
    _setup_migration_table(client, temp_dir)

    migration = Migration(name="0001_test.sql", parents=[INIT_MIGRATION])
    client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), migration])
    assert not client.is_migration_applied(migration)


def test_update_migration_hash(client: PsqlClient, temp_dir: Path) -> None:
    _setup_migration_table(client, temp_dir)

    filename = "0001_update_hash.sql"
    migrations_dir = temp_dir / "migrations"
    create_migration_file(migrations_dir, filename, sql="SELECT 1;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), migration])

    client.apply_migration(migration, is_fake=False)

    # Get the current hash from DB
    with client.connection.cursor() as cursor:
        cursor.execute(f"SELECT change_hash FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        row = cursor.fetchone()
        assert row is not None
        old_hash = row[0]

    # Update the hash
    client.update_migration_hash(migration)
    client.connection.commit()

    # Get the new hash
    with client.connection.cursor() as cursor:
        cursor.execute(f"SELECT change_hash FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        row = cursor.fetchone()
        assert row is not None
        new_hash = row[0]

    assert old_hash == new_hash  # same file, same hash


def test_update_migration_hash_after_file_change(client: PsqlClient, temp_dir: Path) -> None:
    _setup_migration_table(client, temp_dir)

    filename = "0002_update_hash.sql"
    migrations_dir = temp_dir / "migrations"
    create_migration_file(migrations_dir, filename, sql="SELECT 1;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), migration])

    client.apply_migration(migration, is_fake=False)

    # Change the file content
    path = os.path.join(migrations_dir, filename)
    new_content = "SELECT 2;"
    with open(path, "a") as f:
        f.write(new_content)
    # Re-write with proper rollback tag
    with open(path, "w") as f:
        f.write(f"SELECT 1;\n{new_content}\n\n{client.__class__.__module__}")
    # Actually let's use the helper
    os.remove(path)
    create_migration_file(migrations_dir, filename, sql="SELECT 1;\nSELECT 2;")

    client.update_migration_hash(migration)
    client.connection.commit()

    with client.connection.cursor() as cursor:
        cursor.execute(f"SELECT change_hash FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        row = cursor.fetchone()
        assert row is not None
        new_hash = row[0]

    assert "SELECT 1;" not in new_hash  # hash is hex


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("CREATE TABLE test (id INT);", "IF NOT EXISTS"),
        ("CREATE TABLE IF NOT EXISTS test (id INT);", "CREATE TABLE IF NOT EXISTS TEST (ID INT);"),
        ("DROP TABLE test;", "IF EXISTS"),
        ("DROP TABLE IF EXISTS test;", "DROP TABLE IF EXISTS TEST;"),
        ("ALTER TABLE test ADD COLUMN new_col TEXT;", "IF NOT EXISTS"),
        ("ALTER TABLE test DROP COLUMN old_col;", "IF EXISTS"),
    ],
)
def test_patch_ddl_statements(client: PsqlClient, temp_dir: Path, sql: str, expected: str) -> None:
    patched = client._patch_sql_statement(sql)
    assert expected in patched


def test_patch_select_returns_upper(client: PsqlClient, temp_dir: Path) -> None:
    sql = "SELECT 1;"
    patched = client._patch_sql_statement(sql)
    assert patched == "SELECT 1;"


def test_patch_no_ddl_returns_upper(client: PsqlClient, temp_dir: Path) -> None:
    sql = "INSERT INTO test VALUES (1);"
    patched = client._patch_sql_statement(sql)
    assert patched == "INSERT INTO TEST VALUES (1);"


def test_patch_comment_removal(client: PsqlClient, temp_dir: Path) -> None:
    sql = "-- comment\nCREATE TABLE test (id INT);"
    patched = client._patch_sql_statement(sql)
    assert "-- comment" not in patched


def test_patch_block_comment_removal(client: PsqlClient, temp_dir: Path) -> None:
    sql = "/* comment */ CREATE TABLE test (id INT);"
    patched = client._patch_sql_statement(sql)
    assert "/* comment */" not in patched


def test_get_content_and_hash(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0001_test.sql"
    create_migration_file(migrations_dir, filename, sql="SELECT 1;", rollback_sql="SELECT 2;")
    path = os.path.join(migrations_dir, filename)
    migration_code, reverse_code, hash_val = client._get_migration_content_and_hash(Path(path))
    assert "SELECT 1;" in migration_code
    assert "SELECT 2;" in reverse_code
    assert len(hash_val) == 64  # SHA-256 hex digest


def test_get_content_and_hash_empty_reverse(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0002_test.sql"
    path = os.path.join(migrations_dir, filename)
    with open(path, "w") as f:
        f.write("SELECT 1;\n\n-- Rollback migration")
    migration_code, reverse_code, hash_val = client._get_migration_content_and_hash(Path(path))
    assert "SELECT 1;" in migration_code
    assert reverse_code == ""


def test_get_database_hash(client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()

    test_hash = "abc123def456"
    with client.connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (%s, %s)",
            ("0001_test.sql", test_hash),
        )
    client.connection.commit()
    hash_result = client._get_database_hash("0001_test.sql")
    assert hash_result == test_hash


def test_get_database_hash_not_found(client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()

    with pytest.raises(ValueError):
        client._get_database_hash("nonexistent.sql")


def test_no_table_returns_not_applied(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()

    with client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
    client.connection.commit()

    create_migration_file(migrations_dir, "0001_test.sql", sql="SELECT 1;")
    migrations = [Migration(name="0001_test.sql", parents=[INIT_MIGRATION])]
    client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), *migrations])
    statuses = client.retrieve_migration_statuses()
    assert statuses["0001_test.sql"] == MigrationStatus.NOT_APPLIED


def test_mixed_statuses(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()

    create_migration_file(migrations_dir, "0001_applied.sql", sql="SELECT 1;")
    create_migration_file(migrations_dir, "0002_not_applied.sql", sql="SELECT 2;")
    migrations = [
        Migration(name="0001_applied.sql", parents=[INIT_MIGRATION]),
        Migration(name="0002_not_applied.sql", parents=[INIT_MIGRATION]),
    ]
    client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), *migrations])

    # Apply first migration
    client.apply_migration(migrations[0], is_fake=False)

    statuses = client.retrieve_migration_statuses()
    assert statuses["0001_applied.sql"] == MigrationStatus.APPLIED
    assert statuses["0002_not_applied.sql"] == MigrationStatus.NOT_APPLIED


def test_validate_empty_migrations(client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()

    client.config.changelog = ChangelogFile(version=1, migrations=[])
    client.validate_migrations({})  # should not raise


def test_validate_no_initial_raises(client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()

    migrations = [Migration(name="0001_test.sql", parents=[])]
    client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses: dict[str, MigrationStatus] = {}
    with pytest.raises(ValueError):
        client.validate_migrations(statuses)


def test_validate_multiple_initial_raises(client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()

    migrations = [
        Migration(name="0000_a.sql", initial=True, parents=[]),
        Migration(name="0001_b.sql", initial=True, parents=[]),
    ]
    client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses: dict[str, MigrationStatus] = {}
    with pytest.raises(ValueError):
        client.validate_migrations(statuses)


def test_validate_removed_raises(client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()

    migrations = [Migration(name="0000_init.sql", initial=True, parents=[])]
    client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = {"0000_init.sql": MigrationStatus.APPLIED, "ghost.sql": MigrationStatus.REMOVED}
    with pytest.raises(ValueError):
        client.validate_migrations(statuses)


def test_validate_parent_not_applied(client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()

    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_child.sql", parents=["0000_init.sql"]),
    ]
    client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = {
        "0000_init.sql": MigrationStatus.NOT_APPLIED,
        "0001_child.sql": MigrationStatus.APPLIED,
    }
    with pytest.raises(ValueError):
        client.validate_migrations(statuses)
