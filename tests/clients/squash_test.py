from pathlib import Path
from typing import Any

from migrateit.clients._client import SqlClient
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.migration import Migration
from tests.clients._clients_test import MIGRATION_NAME
from tests.conftest import INITIAL_MIGRATION, TEST_MIGRATIONS_TABLE, _create_migration_file, _migration_is_applied


def _migrations_got_squashed(client: SqlClient[Any], migrations: list[str]) -> bool:
    match client.changelog.database:
        case SupportedDatabase.POSTGRES:
            with client.connection.cursor() as cursor:
                query = f"""
SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE}
WHERE migration_name = ANY(%(migrations)s) AND squashed = TRUE
"""
                cursor.execute(query, {"migrations": migrations})
                result = cursor.fetchone()
                assert result is not None
                return result[0] == len(migrations)
        case SupportedDatabase.MYSQL:
            with client.connection.cursor() as cursor:
                query = f"""
SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE}
WHERE migration_name IN ({", ".join(["%s"] * len(migrations))}) AND squashed = TRUE
"""
                cursor.execute(query, migrations)
                result = cursor.fetchone()
                assert result is not None
                return result[0] == len(migrations)
        case SupportedDatabase.SQLITE:
            query = f"""
SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE}
WHERE migration_name IN ({",".join("?" for _ in migrations)}) AND squashed = TRUE
"""
            cursor = client.connection.execute(query, migrations)
            result = cursor.fetchone()
            assert result is not None
            return result[0] == len(migrations)
        case _:
            raise NotImplementedError


def test_squash_migrations_marks_old_as_squashed_and_applies_new(client: SqlClient[Any], temp_dir: Path) -> None:
    """Test squashing applied migrations across databases."""
    migrations_dir = temp_dir / "migrations"

    # Apply the initial migration (fake — table already exists from client fixture)
    client.apply_migration(client.changelog.migrations[0], is_fake=True)

    old_migrations = [MIGRATION_NAME, "0002_migration.sql"]
    new_migration_name = "0012_squashed.sql"

    for fname in old_migrations:
        _create_migration_file(migrations_dir, fname)

    for fname in old_migrations:
        m = Migration(name=fname, parents=[INITIAL_MIGRATION] if fname == old_migrations[0] else [old_migrations[0]])
        client.changelog.migrations.append(m)

    for migration in client.changelog.migrations[1:]:  # skip initial
        client.apply_migration(migration, is_fake=False)

    _create_migration_file(migrations_dir, new_migration_name, sql="-- squashed content")
    new_migration = Migration(name=new_migration_name, parents=old_migrations)

    client.squash_migrations(migrations=old_migrations, new_migration=new_migration)
    assert _migration_is_applied(client, new_migration_name)
    assert _migrations_got_squashed(client, old_migrations)
