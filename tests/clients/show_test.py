from pathlib import Path
from typing import Any

from migrateit.clients._client import SqlClient
from migrateit.models.config import SupportedDatabase
from migrateit.models.migration import Migration, MigrationStatus
from tests.conftest import INITIAL_MIGRATION, MIGRATION_NAME, TEST_MIGRATIONS_TABLE, _mock_create_migration


def _update_migration_hash(client: SqlClient[Any], name: str, hash_value: str) -> None:
    table_name = client._q(TEST_MIGRATIONS_TABLE)
    match client.changelog.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {table_name} SET change_hash = %s WHERE migration_name = %s",
                    (hash_value, name),
                )
        case SupportedDatabase.SQLITE:
            client.connection.execute(
                f"UPDATE {table_name} SET change_hash = ? WHERE migration_name = ?",
                (hash_value, name),
            )
        case _:
            raise NotImplementedError
    client.connection.commit()


def test_show_migrations_not_applied(client: SqlClient[Any], temp_dir: Path) -> None:
    _mock_create_migration(temp_dir, client.changelog.database)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)

    result = client.retrieve_migration_statuses()
    assert result[migration.name] == MigrationStatus.NOT_APPLIED


def test_show_migrations_applied(client: SqlClient[Any], temp_dir: Path) -> None:
    _mock_create_migration(temp_dir, client.changelog.database)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)
    client.apply_migration(migration, is_fake=True)

    result = client.retrieve_migration_statuses()
    assert result[migration.name] == MigrationStatus.APPLIED


def test_show_migrations_conflict(client: SqlClient[Any], temp_dir: Path) -> None:
    _mock_create_migration(temp_dir, client.changelog.database)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.changelog.migrations.append(migration)
    client.apply_migration(migration, is_fake=True)
    _update_migration_hash(client, MIGRATION_NAME, "different_hash")  # mismatch

    result = client.retrieve_migration_statuses()
    assert result[migration.name] == MigrationStatus.CONFLICT


def test_show_migrations_removed(client: SqlClient[Any], temp_dir: Path) -> None:
    _mock_create_migration(temp_dir, client.changelog.database)
    migration = Migration(name=MIGRATION_NAME, parents=(INITIAL_MIGRATION,))
    client.apply_migration(migration, is_fake=True)

    result = client.retrieve_migration_statuses()
    assert result[migration.name] == MigrationStatus.REMOVED
