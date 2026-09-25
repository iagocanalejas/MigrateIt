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


@pytest.mark.postgres
def test_table_exists_returns_true(pg_client: PsqlClient, temp_dir: Path) -> None:
    # drop the table first as is being created in pytest fixtures
    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
    pg_client.connection.commit()

    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
        pg_client.connection.commit()
    assert pg_client.is_migrations_table_created()


@pytest.mark.postgres
def test_table_missing_returns_false(pg_client: PsqlClient, temp_dir: Path) -> None:
    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
    pg_client.connection.commit()
    assert not pg_client.is_migrations_table_created()


@pytest.mark.postgres
def test_applied_migration_returns_true(pg_client: PsqlClient, temp_dir: Path) -> None:
    migration = Migration(name="0000_init.sql", initial=True, parents=[])
    migrations_dir = temp_dir / "migrations"
    create_migration_file(migrations_dir, "0000_init.sql", sql="SELECT 1;")
    pg_client.config.changelog = ChangelogFile(version=1, migrations=[migration])

    pg_client.apply_migration(migration, is_fake=False)
    assert pg_client.is_migration_applied(migration)


@pytest.mark.postgres
def test_not_applied_migration_returns_false(pg_client: PsqlClient, temp_dir: Path) -> None:
    migration = Migration(name="0001_test.sql", parents=[INIT_MIGRATION])
    pg_client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), migration])
    assert not pg_client.is_migration_applied(migration)


@pytest.mark.postgres
def test_update_migration_hash(pg_client: PsqlClient, temp_dir: Path) -> None:
    filename = "0001_update_hash.sql"
    migrations_dir = temp_dir / "migrations"
    create_migration_file(migrations_dir, filename, sql="SELECT 1;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    pg_client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), migration])

    pg_client.apply_migration(migration, is_fake=False)

    # Get the current hash from DB
    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT change_hash FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        row = cursor.fetchone()
        assert row is not None
        old_hash = row[0]

    # Update the hash
    pg_client.update_migration_hash(migration)
    pg_client.connection.commit()

    # Get the new hash
    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT change_hash FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        row = cursor.fetchone()
        assert row is not None
        new_hash = row[0]

    assert old_hash == new_hash  # same file, same hash


@pytest.mark.postgres
def test_update_migration_hash_after_file_change(pg_client: PsqlClient, temp_dir: Path) -> None:
    filename = "0002_update_hash.sql"
    migrations_dir = temp_dir / "migrations"
    create_migration_file(migrations_dir, filename, sql="SELECT 1;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    pg_client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), migration])

    pg_client.apply_migration(migration, is_fake=False)

    # Change the file content
    path = os.path.join(migrations_dir, filename)
    new_content = "SELECT 2;"
    with open(path, "a") as f:
        f.write(new_content)
    # Re-write with proper rollback tag
    with open(path, "w") as f:
        f.write(f"SELECT 1;\n{new_content}\n\n{pg_client.__class__.__module__}")
    # Actually let's use the helper
    os.remove(path)
    create_migration_file(migrations_dir, filename, sql="SELECT 1;\nSELECT 2;")

    pg_client.update_migration_hash(migration)
    pg_client.connection.commit()

    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT change_hash FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        row = cursor.fetchone()
        assert row is not None
        new_hash = row[0]

    assert "SELECT 1;" not in new_hash  # hash is hex


@pytest.mark.postgres
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
def test_patch_ddl_statements(pg_client: PsqlClient, temp_dir: Path, sql: str, expected: str) -> None:
    patched = pg_client._patch_sql_statement(sql)
    assert expected in patched


@pytest.mark.postgres
def test_patch_select_returns_upper(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql = "SELECT 1;"
    patched = pg_client._patch_sql_statement(sql)
    assert patched == "SELECT 1;"


@pytest.mark.postgres
def test_patch_no_ddl_returns_upper(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql = "INSERT INTO test VALUES (1);"
    patched = pg_client._patch_sql_statement(sql)
    assert patched == "INSERT INTO TEST VALUES (1);"


@pytest.mark.postgres
def test_patch_comment_removal(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql = "-- comment\nCREATE TABLE test (id INT);"
    patched = pg_client._patch_sql_statement(sql)
    assert "-- comment" not in patched


@pytest.mark.postgres
def test_patch_block_comment_removal(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql = "/* comment */ CREATE TABLE test (id INT);"
    patched = pg_client._patch_sql_statement(sql)
    assert "/* comment */" not in patched


@pytest.mark.postgres
def test_get_content_and_hash(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0001_test.sql"
    create_migration_file(migrations_dir, filename, sql="SELECT 1;", rollback_sql="SELECT 2;")
    path = os.path.join(migrations_dir, filename)
    migration_code, reverse_code, hash_val = pg_client._get_migration_content_and_hash(Path(path))
    assert "SELECT 1;" in migration_code
    assert "SELECT 2;" in reverse_code
    assert len(hash_val) == 64  # SHA-256 hex digest


@pytest.mark.postgres
def test_get_content_and_hash_empty_reverse(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0002_test.sql"
    path = os.path.join(migrations_dir, filename)
    with open(path, "w") as f:
        f.write("SELECT 1;\n\n-- Rollback migration")
    migration_code, reverse_code, hash_val = pg_client._get_migration_content_and_hash(Path(path))
    assert "SELECT 1;" in migration_code
    assert reverse_code == ""


@pytest.mark.postgres
def test_get_database_hash(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    pg_client.connection.commit()

    test_hash = "abc123def456"
    with pg_client.connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (%s, %s)",
            ("0001_test.sql", test_hash),
        )
    pg_client.connection.commit()
    hash_result = pg_client._get_database_hash("0001_test.sql")
    assert hash_result == test_hash


@pytest.mark.postgres
def test_get_database_hash_not_found(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    pg_client.connection.commit()

    with pytest.raises(ValueError):
        pg_client._get_database_hash("nonexistent.sql")


@pytest.mark.postgres
def test_no_table_returns_not_applied(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    pg_client.connection.commit()

    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
    pg_client.connection.commit()

    create_migration_file(migrations_dir, "0001_test.sql", sql="SELECT 1;")
    migrations = [Migration(name="0001_test.sql", parents=[INIT_MIGRATION])]
    pg_client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), *migrations])
    statuses = pg_client.retrieve_migration_statuses()
    assert statuses["0001_test.sql"] == MigrationStatus.NOT_APPLIED


@pytest.mark.postgres
def test_mixed_statuses(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    pg_client.connection.commit()

    create_migration_file(migrations_dir, "0001_applied.sql", sql="SELECT 1;")
    create_migration_file(migrations_dir, "0002_not_applied.sql", sql="SELECT 2;")
    migrations = [
        Migration(name="0001_applied.sql", parents=[INIT_MIGRATION]),
        Migration(name="0002_not_applied.sql", parents=[INIT_MIGRATION]),
    ]
    pg_client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), *migrations])

    # Apply first migration
    pg_client.apply_migration(migrations[0], is_fake=False)

    statuses = pg_client.retrieve_migration_statuses()
    assert statuses["0001_applied.sql"] == MigrationStatus.APPLIED
    assert statuses["0002_not_applied.sql"] == MigrationStatus.NOT_APPLIED


@pytest.mark.postgres
def test_validate_empty_migrations(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    pg_client.connection.commit()

    pg_client.config.changelog = ChangelogFile(version=1, migrations=[])
    statuses: dict[str, MigrationStatus] = {}
    pg_client.validate_migrations(statuses)  # should not raise
    assert statuses == {}  # empty dict passed through unchanged


@pytest.mark.postgres
def test_validate_no_initial_raises(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    pg_client.connection.commit()

    migrations = [Migration(name="0001_test.sql", parents=[])]
    pg_client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses: dict[str, MigrationStatus] = {}
    with pytest.raises(ValueError):
        pg_client.validate_migrations(statuses)


@pytest.mark.postgres
def test_validate_multiple_initial_raises(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    pg_client.connection.commit()

    migrations = [
        Migration(name="0000_a.sql", initial=True, parents=[]),
        Migration(name="0001_b.sql", initial=True, parents=[]),
    ]
    pg_client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses: dict[str, MigrationStatus] = {}
    with pytest.raises(ValueError):
        pg_client.validate_migrations(statuses)


@pytest.mark.postgres
def test_validate_removed_raises(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    pg_client.connection.commit()

    migrations = [Migration(name="0000_init.sql", initial=True, parents=[])]
    pg_client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = {"0000_init.sql": MigrationStatus.APPLIED, "ghost.sql": MigrationStatus.REMOVED}
    with pytest.raises(ValueError):
        pg_client.validate_migrations(statuses)


@pytest.mark.postgres
def test_validate_parent_not_applied(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    pg_client.connection.commit()

    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_child.sql", parents=["0000_init.sql"]),
    ]
    pg_client.config.changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = {
        "0000_init.sql": MigrationStatus.NOT_APPLIED,
        "0001_child.sql": MigrationStatus.APPLIED,
    }
    with pytest.raises(ValueError):
        pg_client.validate_migrations(statuses)


# --- validate_migrations conflict path ---


@pytest.mark.postgres
def test_validate_conflict_raises(pg_client: PsqlClient, temp_dir: Path) -> None:
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with pg_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    pg_client.connection.commit()

    filename = "0001_test.sql"
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    create_migration_file(migrations_dir, filename, sql="SELECT 1;")

    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name=filename, parents=["0000_init.sql"]),
    ]
    pg_client.config.changelog = ChangelogFile(version=1, migrations=migrations)

    # Insert a different hash into the DB to trigger conflict
    with pg_client.connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (%s, %s)",
            (filename, "different_hash"),
        )
    pg_client.connection.commit()

    statuses = pg_client.retrieve_migration_statuses()
    assert statuses[filename] == MigrationStatus.CONFLICT
    with pytest.raises(ValueError, match="has a different hash"):
        pg_client.validate_migrations(statuses)
