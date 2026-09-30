from pathlib import Path

import pytest

from migrateit.clients.mysql import MySqlClient
from migrateit.models import Migration
from tests.conftest import TEST_MIGRATIONS_TABLE, create_migration_file

TEST_TABLE = "test_entity"


@pytest.mark.mysql
def test_apply_migration_success(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test applying a migration and checking status."""
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
    mysql_client.changelog.migrations.append(migration)

    mysql_client.apply_migration(migration, is_fake=False)

    # Check it was inserted into the migrations table
    with mysql_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        result = cursor.fetchone()
        assert result[0] if result else None == 1  # type: ignore

        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (TEST_TABLE,))
        result = cursor.fetchone()
        assert result[0] if result else None  # type: ignore

        # Clean up any test tables created by apply_test.py tests
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")


@pytest.mark.mysql
def test_apply_migration_fake(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test fake applying a migration (marks as applied without executing SQL)."""
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
    mysql_client.changelog.migrations.append(migration)

    mysql_client.apply_migration(migration, is_fake=True)

    # Check it was inserted into the migrations table
    with mysql_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        result = cursor.fetchone()
        assert result[0] if result else None == 1  # type: ignore

        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (TEST_TABLE,))
        result = cursor.fetchone()
        assert not (result[0] if result else None)  # type: ignore


@pytest.mark.mysql
def test_apply_migration_undo_success(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test rolling back a migration."""
    filename = "0005_undoable.sql"
    migrations_dir = temp_dir / "migrations"

    create_migration_file(
        migrations_dir,
        filename,
        sql=f"CREATE TABLE IF NOT EXISTS {TEST_TABLE} (id SERIAL PRIMARY KEY);",
        rollback_sql=f"DROP TABLE IF EXISTS {TEST_TABLE};",
    )

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    mysql_client.changelog.migrations.append(migration)

    mysql_client.apply_migration(migration, is_fake=False)
    mysql_client.apply_migration(migration, is_fake=False, is_rollback=True)

    with mysql_client.connection.cursor() as cursor:
        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (TEST_TABLE,))
        result = cursor.fetchone()
        assert not (result[0] if result else None)  # type: ignore


@pytest.mark.mysql
def test_apply_migration_file_missing(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test applying a migration with missing file."""
    migration = Migration(name="not_found.sql", parents=["0000_migrateit.sql"])

    with pytest.raises(FileNotFoundError):
        mysql_client.apply_migration(migration)


@pytest.mark.mysql
def test_apply_migration_wrong_extension(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test applying a migration with wrong extension."""
    import os

    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0002_wrong_ext.txt"
    path = migrations_dir / filename
    path.write_text("SELECT 1;")

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    mysql_client.changelog.migrations.append(migration)

    with pytest.raises(FileNotFoundError):
        mysql_client.apply_migration(migration)


@pytest.mark.mysql
def test_apply_migration_already_applied(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test applying an already applied migration."""
    filename = "0003_applied.sql"
    migrations_dir = temp_dir / "migrations"

    create_migration_file(migrations_dir, filename)

    migration = Migration(name=filename, parents=["0000_migrateit.sql"])
    mysql_client.changelog.migrations.append(migration)

    mysql_client.apply_migration(migration, is_fake=False)
    with pytest.raises(ValueError, match="already applied, cannot apply it again"):
        mysql_client.apply_migration(migration, is_fake=False)


@pytest.mark.mysql
def test_apply_migration_rollback_on_error(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test that MySQL errors trigger a rollback."""
    mysql_client.changelog.migrations.append(
        Migration(name="0001_bad.sql", initial=False, parents=["0000_migrateit.sql"])
    )
    migration = mysql_client.changelog.migrations[1]
    with pytest.raises(Exception):
        mysql_client.apply_migration(migration)


@pytest.mark.mysql
def test_apply_migration_rollback_on_mysql_error(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test that MySQL errors trigger a rollback."""
    create_migration_file(
        temp_dir / "migrations",
        "0001_bad.sql",
        sql="NOT VALID SQL AT ALL !!!",
        rollback_sql="DROP TABLE test;",
    )

    mysql_client.changelog.migrations.append(
        Migration(name="0001_bad.sql", initial=False, parents=["0000_migrateit.sql"])
    )
    migration = mysql_client.changelog.migrations[1]

    with pytest.raises(Exception):
        mysql_client.apply_migration(migration)
