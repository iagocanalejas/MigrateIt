from pathlib import Path

import pytest
from psycopg import sql

from migrateit.clients import PsqlClient
from migrateit.models import Migration
from tests.conftest import INIT_MIGRATION, TEST_MIGRATIONS_TABLE, create_migration_file

TEST_TABLE = "test_entity"


@pytest.mark.postgres
def test_squash_migrations_marks_old_as_squashed_and_applies_new_fake(
    pg_client: PsqlClient,
    temp_dir: Path,
) -> None:
    """Test squashing applied migrations across databases."""
    migrations_dir = temp_dir / "migrations"

    # Create the migrateit initial migration file
    sql_create, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    create_migration_file(migrations_dir, INIT_MIGRATION, sql=sql_create)

    # Apply the initial migration (fake — table already exists from pg_client fixture)
    pg_client.apply_migration(pg_client.changelog.migrations[0], is_fake=True)

    old_migrations = ["0010_one.sql", "0011_two.sql"]
    new_migration_name = "0012_squashed.sql"

    for fname in old_migrations:
        create_migration_file(migrations_dir, fname, sql="SELECT 1;")

    for fname in old_migrations:
        pg_client.changelog.migrations.append(
            Migration(
                name=fname,
                parents=[INIT_MIGRATION] if fname == old_migrations[0] else [old_migrations[0]],
            )
        )

    for migration in pg_client.changelog.migrations[1:]:  # skip initial
        pg_client.apply_migration(migration, is_fake=False)

    create_migration_file(migrations_dir, new_migration_name, sql="-- squashed content")
    new_migration = Migration(name=new_migration_name, parents=old_migrations)

    pg_client.squash_migrations(migrations=old_migrations, new_migration=new_migration)

    with pg_client.connection.cursor() as cursor:
        query = sql.SQL("""
SELECT COUNT(*) FROM {}
WHERE migration_name = ANY(%(migrations)s) AND squashed = TRUE
        """)
        cursor.execute(query.format(sql.Identifier(TEST_MIGRATIONS_TABLE)), {"migrations": old_migrations})
        result = cursor.fetchone()
        assert (result[0] if result else None) == len(old_migrations)

        # Assert new migration is recorded
        query = sql.SQL("""
SELECT COUNT(*) FROM {} WHERE migration_name = %s
        """)
        cursor.execute(query.format(sql.Identifier(TEST_MIGRATIONS_TABLE)), (new_migration_name,))
        result = cursor.fetchone()
        assert (result[0] if result else None) == 1
