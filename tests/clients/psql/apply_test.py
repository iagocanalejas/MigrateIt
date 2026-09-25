import os
from pathlib import Path

import pytest

from migrateit.clients import PsqlClient
from migrateit.models import Migration
from migrateit.tree import ROLLBACK_SPLIT_TAG
from tests.conftest import TEST_MIGRATIONS_TABLE, create_migration_file

TEST_TABLE = "test_entity"


@pytest.mark.postgres
def test_apply_migration_success(pg_client: PsqlClient, temp_dir: Path) -> None:
    """Test applying a migration and checking it's recorded in the DB."""
    filename = "0001_test_table.sql"
    migrations_dir = temp_dir / "migrations"

    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    pg_client.changelog.migrations.append(migration)

    pg_client.apply_migration(migration, is_fake=False)

    # Check it was inserted into the migrations table
    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        result = cursor.fetchone()
        assert result[0] if result else None == 1

        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (TEST_TABLE,))
        result = cursor.fetchone()
        assert result[0] if result else None

        # Clean up any test tables created by apply_test.py tests
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")


@pytest.mark.postgres
def test_apply_migration_fake(pg_client: PsqlClient, temp_dir: Path) -> None:
    """Test fake applying a migration (recorded but SQL not executed)."""
    filename = "0002_fake_table.sql"
    migrations_dir = temp_dir / "migrations"

    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    pg_client.changelog.migrations.append(migration)

    pg_client.apply_migration(migration, is_fake=True)

    # Check it was inserted into the migrations table
    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        result = cursor.fetchone()
        assert result[0] if result else None == 1

        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (TEST_TABLE,))
        result = cursor.fetchone()
        assert not (result[0] if result else None)


@pytest.mark.postgres
def test_apply_migration_file_missing(pg_client: PsqlClient) -> None:
    """Test applying a migration with a missing file."""
    migration = Migration(name="not_found.sql", parents=["0000_migrateit.sql"])
    pg_client.changelog.migrations.append(migration)

    with pytest.raises(FileNotFoundError):
        pg_client.apply_migration(migration, is_fake=False)


@pytest.mark.postgres
def test_apply_migration_already_applied(pg_client: PsqlClient, temp_dir: Path) -> None:
    """Test applying an already applied migration raises ValueError."""
    filename = "0003_applied.sql"
    migrations_dir = temp_dir / "migrations"

    create_migration_file(migrations_dir, filename, sql="SELECT 1;")

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    pg_client.changelog.migrations.append(migration)

    pg_client.apply_migration(migration, is_fake=False)
    with pytest.raises(ValueError):
        pg_client.apply_migration(migration, is_fake=False)


@pytest.mark.postgres
def test_apply_migration_wrong_extension(pg_client: PsqlClient, temp_dir: Path) -> None:
    """Test applying a migration with wrong extension raises FileNotFoundError."""
    filename = "0004_wrong_ext.txt"
    migrations_dir = temp_dir / "migrations"

    create_migration_file(migrations_dir, filename, sql="SELECT 1;")

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    pg_client.changelog.migrations.append(migration)

    with pytest.raises(FileNotFoundError):
        pg_client.apply_migration(migration, is_fake=False)


@pytest.mark.postgres
def test_apply_migration_undo_success(pg_client: PsqlClient, temp_dir: Path) -> None:
    """Test rolling back a migration."""
    filename = "0005_undoable.sql"
    migrations_dir = temp_dir / "migrations"

    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE} (id SERIAL PRIMARY KEY);
            {ROLLBACK_SPLIT_TAG}
            DROP TABLE IF EXISTS {TEST_TABLE};
        """,
    )

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    pg_client.changelog.migrations.append(migration)

    pg_client.apply_migration(migration, is_fake=False)
    pg_client.apply_migration(migration, is_fake=False, is_rollback=True)

    with pg_client.connection.cursor() as cursor:
        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (TEST_TABLE,))
        result = cursor.fetchone()
        assert not (result[0] if result else None)


@pytest.mark.postgres
def test_apply_migration_undo_fake(pg_client: PsqlClient, temp_dir: Path) -> None:
    """Test fake rolling back a migration."""
    filename = "0006_fake_undo.sql"
    migrations_dir = temp_dir / "migrations"

    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            -- some forward SQL
            SELECT 1;
            {ROLLBACK_SPLIT_TAG}
            -- reverse SQL
            SELECT 2;
        """,
    )

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    pg_client.changelog.migrations.append(migration)

    pg_client.apply_migration(migration, is_fake=False)
    pg_client.apply_migration(migration, is_fake=True, is_rollback=True)

    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        result = cursor.fetchone()
        assert (result[0] if result else None) == 0


@pytest.mark.postgres
def test_apply_migration_undo_missing_reverse_sql(pg_client: PsqlClient, temp_dir: Path) -> None:
    """Test applying a rollback without reverse SQL raises ValueError."""
    filename = "0007_missing_reverse.sql"
    migrations_dir = temp_dir / "migrations"

    # Write raw file (no rollback tag) — must not use create_migration_file
    migrations_dir.mkdir(parents=True, exist_ok=True)
    with open(os.path.join(migrations_dir, filename), "w") as f:
        f.write("SELECT 1;")

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    pg_client.changelog.migrations.append(migration)

    with pytest.raises(ValueError):
        pg_client.apply_migration(migration, is_fake=False)


@pytest.mark.postgres
def test_apply_migration_undo_not_applied(pg_client: PsqlClient, temp_dir: Path) -> None:
    """Test rolling back a migration that was never applied raises ValueError."""
    filename = "0008_not_applied.sql"
    migrations_dir = temp_dir / "migrations"

    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            SELECT 1;
            {ROLLBACK_SPLIT_TAG}
            SELECT 2;
        """,
    )

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    pg_client.changelog.migrations.append(migration)

    with pytest.raises(ValueError):
        pg_client.apply_migration(migration, is_fake=False, is_rollback=True)


@pytest.mark.postgres
def test_apply_migration_fake_and_undo_combination(pg_client: PsqlClient, temp_dir: Path) -> None:
    """Test fake applying then faking the rollback."""
    filename = "0009_fake_undo.sql"
    migrations_dir = temp_dir / "migrations"

    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            SELECT 1;
            {ROLLBACK_SPLIT_TAG}
            SELECT 2;
        """,
    )

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    pg_client.changelog.migrations.append(migration)

    pg_client.apply_migration(migration, is_fake=True)
    pg_client.apply_migration(migration, is_fake=True, is_rollback=True)

    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        result = cursor.fetchone()
        assert (result[0] if result else None) == 0
