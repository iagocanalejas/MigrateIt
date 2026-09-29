from pathlib import Path

import pytest

from migrateit.clients.sqlite import SqliteClient
from migrateit.models import Migration
from tests.conftest import INIT_MIGRATION, TEST_MIGRATIONS_TABLE, create_migration_file


@pytest.mark.sqlite
def test_squash_migrations_marks_old_as_squashed_and_applies_new(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test squashing applied migrations across databases."""
    migrations_dir = temp_dir / "migrations"

    # Create the migrateit initial migration file
    sql_create, _ = SqliteClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    create_migration_file(migrations_dir, INIT_MIGRATION, sql=sql_create)

    # Apply the initial migration (fake — table already exists from pg_client fixture)
    sqlite_client.apply_migration(sqlite_client.changelog.migrations[0], is_fake=True)

    old_migrations = ["0010_one.sql", "0011_two.sql"]
    new_migration_name = "0012_squashed.sql"

    for fname in old_migrations:
        create_migration_file(migrations_dir, fname)

    for fname in old_migrations:
        sqlite_client.changelog.migrations.append(
            Migration(
                name=fname,
                parents=[INIT_MIGRATION] if fname == old_migrations[0] else [old_migrations[0]],
            )
        )

    for migration in sqlite_client.changelog.migrations[1:]:  # skip initial
        sqlite_client.apply_migration(migration, is_fake=False)

    create_migration_file(migrations_dir, new_migration_name, sql="-- squashed content")
    new_migration = Migration(name=new_migration_name, parents=old_migrations)

    sqlite_client.squash_migrations(migrations=old_migrations, new_migration=new_migration)

    query = f"""
SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE}
WHERE migration_name IN ({",".join("?" for _ in old_migrations)}) AND squashed = TRUE
    """
    cursor = sqlite_client.connection.execute(query, old_migrations)
    result = cursor.fetchone()
    assert (result[0] if result else None) == len(old_migrations)

    # Assert new migration is recorded
    query = f"""
SELECT COUNT(*) FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = ?
    """
    cursor = sqlite_client.connection.execute(query, (new_migration_name,))
    result = cursor.fetchone()
    assert (result[0] if result else None) == 1
