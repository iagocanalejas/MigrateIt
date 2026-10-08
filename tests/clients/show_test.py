from pathlib import Path
from typing import Any

from migrateit.clients._client import SqlClient
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.migration import Migration, MigrationStatus
from tests.clients.apply_test import MIGRATION_NAME
from tests.conftest import INITIAL_MIGRATION, TEST_MIGRATIONS_TABLE, TEST_TABLE, _create_migration_file


def _update_migration_hash(client: SqlClient[Any], name: str, hash_value: str) -> None:
    match client.changelog.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {TEST_MIGRATIONS_TABLE} SET change_hash = %s WHERE migration_name = %s",
                    (hash_value, name),
                )
        case SupportedDatabase.SQLITE:
            client.connection.execute(
                f"UPDATE {TEST_MIGRATIONS_TABLE} SET change_hash = ? WHERE migration_name = ?",
                (hash_value, name),
            )
        case _:
            raise NotImplementedError
    client.connection.commit()


def test_show_migrations_not_applied(client: SqlClient[Any], temp_dir: Path) -> None:
    result = client.retrieve_migration_statuses()
    assert result[INITIAL_MIGRATION] == MigrationStatus.NOT_APPLIED


def test_show_migrations_applied(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )

    migration_applied = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration_applied)
    client.apply_migration(migration_applied, is_fake=True)

    result = client.retrieve_migration_statuses()
    assert result[migration_applied.name] == MigrationStatus.APPLIED


def test_show_migrations_conflict(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )

    migration_applied = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration_applied)
    client.apply_migration(migration_applied, is_fake=True)
    _update_migration_hash(client, MIGRATION_NAME, "different_hash")  # mismatch

    result = client.retrieve_migration_statuses()

    assert result[migration_applied.name] == MigrationStatus.CONFLICT


def test_show_migrations_removed(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"

    _create_migration_file(
        migrations_dir,
        MIGRATION_NAME,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )

    migration_applied = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.apply_migration(migration_applied, is_fake=True)

    result = client.retrieve_migration_statuses()

    assert result[migration_applied.name] == MigrationStatus.REMOVED
