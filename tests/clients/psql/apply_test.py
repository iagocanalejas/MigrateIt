import os
from pathlib import Path

import pytest

from migrateit.clients import PsqlClient
from migrateit.models import Migration
from migrateit.tree import ROLLBACK_SPLIT_TAG
from tests.conftest import INIT_MIGRATION, TEST_MIGRATIONS_TABLE, create_empty_changelog, create_migration_file

TEST_TABLE = "test_entity"


def test_apply_migration_success(client: PsqlClient, temp_dir: Path) -> None:
    filename = "0000_init.sql"
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

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
    changelog = create_empty_changelog(temp_dir)
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    changelog.migrations.append(migration)
    client.config.changelog = changelog

    client.apply_migration(migration, is_fake=False)

    # Check it was inserted into the table
    with client.connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        result = cursor.fetchone()
        assert result[0] if result else None == 1

        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (TEST_TABLE,))
        result = cursor.fetchone()
        assert result[0] if result else None

        # Clean up any test tables created by apply_test.py tests
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")


def test_apply_migration_fake(client: PsqlClient, temp_dir: Path) -> None:
    filename = "0000_init.sql"
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

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
    changelog = create_empty_changelog(temp_dir)
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    changelog.migrations.append(migration)
    client.config.changelog = changelog

    client.apply_migration(migration, is_fake=True)

    # Check it was inserted into the table
    with client.connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        result = cursor.fetchone()
        assert result[0] if result else None == 1

        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (TEST_TABLE,))
        result = cursor.fetchone()
        assert not (result[0] if result else None)


def test_apply_migration_file_missing(client: PsqlClient, temp_dir: Path) -> None:
    client.config.changelog = create_empty_changelog(temp_dir)
    migration = Migration(name="not_found.sql", parents=[INIT_MIGRATION])

    with pytest.raises(FileNotFoundError):
        client.apply_migration(migration, is_fake=False)


def test_apply_migration_already_applied(client: PsqlClient, temp_dir: Path) -> None:
    filename = "0001_applied.sql"
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    create_migration_file(migrations_dir, filename, sql="SELECT 1;")
    changelog = create_empty_changelog(temp_dir)
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    changelog.migrations.append(migration)

    client.config.changelog = changelog
    client.apply_migration(migration, is_fake=False)
    with pytest.raises(ValueError):
        client.apply_migration(migration, is_fake=False)


def test_apply_migration_wrong_extension(client: PsqlClient, temp_dir: Path) -> None:
    filename = "0002_wrong_ext.txt"
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    create_migration_file(migrations_dir, filename, sql="SELECT 1;")
    changelog = create_empty_changelog(temp_dir)
    migration = Migration(name=filename, parents=[INIT_MIGRATION])

    client.config.changelog = changelog
    with pytest.raises(FileNotFoundError):
        client.apply_migration(migration, is_fake=False)


def test_apply_migration_undo_success(client: PsqlClient, temp_dir: Path) -> None:
    filename = "0003_undoable.sql"
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE} (id SERIAL PRIMARY KEY);
            {ROLLBACK_SPLIT_TAG}
            DROP TABLE IF EXISTS {TEST_TABLE};
        """,
    )
    changelog = create_empty_changelog(temp_dir)
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    changelog.migrations.append(migration)
    client.config.changelog = changelog

    client.apply_migration(migration, is_fake=False)
    client.apply_migration(migration, is_fake=False, is_rollback=True)

    with client.connection.cursor() as cursor:
        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (TEST_TABLE,))
        result = cursor.fetchone()
        assert not (result[0] if result else None)


def test_apply_migration_undo_fake(client: PsqlClient, temp_dir: Path) -> None:
    filename = "0004_undo_fake.sql"
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

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
    changelog = create_empty_changelog(temp_dir)
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    changelog.migrations.append(migration)
    client.config.changelog = changelog

    client.apply_migration(migration, is_fake=False)
    client.apply_migration(migration, is_fake=True, is_rollback=True)

    with client.connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        result = cursor.fetchone()
        assert (result[0] if result else None) == 0


def test_apply_migration_undo_missing_reverse_sql(client: PsqlClient, temp_dir: Path) -> None:
    filename = "0005_missing_reverse.sql"
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    path = os.path.join(migrations_dir, filename)
    with open(path, "w") as f:
        f.write("SELECT 1;")  # no ROLLBACK_SPLIT_TAG

    changelog = create_empty_changelog(temp_dir)
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    changelog.migrations.append(migration)
    client.config.changelog = changelog

    with pytest.raises(ValueError):
        client.apply_migration(migration, is_fake=False)


def test_apply_migration_undo_not_applied(client: PsqlClient, temp_dir: Path) -> None:
    filename = "0006_not_applied.sql"
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            SELECT 1;
            {ROLLBACK_SPLIT_TAG}
            SELECT 2;
        """,
    )
    changelog = create_empty_changelog(temp_dir)
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    changelog.migrations.append(migration)
    client.config.changelog = changelog

    with pytest.raises(ValueError):
        client.apply_migration(migration, is_fake=False, is_rollback=True)


def test_apply_migration_fake_and_undo_combination(client: PsqlClient, temp_dir: Path) -> None:
    filename = "0007_fake_undo.sql"
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            SELECT 1;
            {ROLLBACK_SPLIT_TAG}
            SELECT 2;
        """,
    )
    changelog = create_empty_changelog(temp_dir)
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    changelog.migrations.append(migration)
    client.config.changelog = changelog

    client.apply_migration(migration, is_fake=True)
    client.apply_migration(migration, is_fake=True, is_rollback=True)

    with client.connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        result = cursor.fetchone()
        assert (result[0] if result else None) == 0
