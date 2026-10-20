from pathlib import Path
from typing import Any

from migrateit.clients._client import SqlClient
from migrateit.models.config import SupportedDatabase
from migrateit.models.migration import Migration
from tests.unit.conftest import (
    INITIAL_MIGRATION,
    MIGRATION_NAME,
    TEST_MIGRATIONS_TABLE,
    _create_migration_file,
    _migration_is_applied,
)


def _migrations_got_squashed(client: SqlClient[Any], migrations: list[str]) -> bool:
    table_name = client._q(TEST_MIGRATIONS_TABLE)
    match client.database:
        case SupportedDatabase.POSTGRES:
            with client.connection.cursor() as cursor:
                query = f"""
SELECT COUNT(*) FROM {table_name}
WHERE migration_name = ANY(%s) AND squashed = TRUE
"""
                cursor.execute(query, (migrations,))
                result = cursor.fetchone()
                assert result is not None
                return result[0] == len(migrations)
        case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                query = f"""
SELECT COUNT(*) FROM {table_name}
WHERE migration_name IN ({", ".join(["%s"] * len(migrations))}) AND squashed = TRUE
"""
                cursor.execute(query, migrations)
                result = cursor.fetchone()
                assert result is not None
                return result[0] == len(migrations)
        case SupportedDatabase.SQLITE:
            query = f"""
SELECT COUNT(*) FROM {table_name}
WHERE migration_name IN ({",".join("?" for _ in migrations)}) AND squashed = TRUE
"""
            cursor = client.connection.execute(query, migrations)
            result = cursor.fetchone()
            assert result is not None
            return result[0] == len(migrations)
        case SupportedDatabase.ORACLE:
            with client.connection.cursor() as cursor:
                placeholders = ", ".join(f":{i + 1}" for i in range(len(migrations)))
                query = f"""
SELECT COUNT(*) FROM {table_name}
WHERE migration_name IN ({placeholders}) AND squashed = 1
"""
                cursor.execute(query, migrations)
                result = cursor.fetchone()
                assert result is not None
                return result[0] == len(migrations)
        case _:  # pragma: no cover
            raise NotImplementedError


def test_squash_migrations_marks_old_as_squashed_and_applies_new(client: SqlClient[Any], temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"

    old_migrations = (MIGRATION_NAME, "0002_migration.sql")
    new_migration_name = "0012_squashed.sql"

    for fname in old_migrations:
        _create_migration_file(migrations_dir, fname)

    for fname in old_migrations:
        m = Migration(name=fname, parents=(INITIAL_MIGRATION,) if fname == old_migrations[0] else (old_migrations[0],))
        client.changelog.migrations.append(m)

    for migration in client.changelog.migrations[1:]:  # skip initial
        client.apply_migration(migration, is_fake=False)

    _create_migration_file(migrations_dir, new_migration_name, sql="-- squashed content")
    new_migration = Migration(name=new_migration_name, parents=old_migrations)

    client.squash_migrations(migrations=list(old_migrations), new_migration=new_migration)
    assert _migration_is_applied(client, new_migration_name)
    assert _migrations_got_squashed(client, list(old_migrations))
