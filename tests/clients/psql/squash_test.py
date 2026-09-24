import os
from pathlib import Path

from psycopg import sql

from migrateit.clients import PsqlClient
from migrateit.models import Migration
from tests.conftest import INIT_MIGRATION, TEST_MIGRATIONS_TABLE, create_empty_changelog, create_migration_file

TEST_TABLE = "test_entity"


def test_squash_migrations_marks_old_as_squashed_and_applies_new_fake(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    sql_create, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    create_migration_file(migrations_dir, INIT_MIGRATION, sql=sql_create)

    with client.connection.cursor() as cursor:
        cursor.execute(sql_create)  # pyright: ignore
    client.connection.commit()

    old_migrations = ["0010_one.sql", "0011_two.sql"]
    new_migration_name = "0012_squashed.sql"

    for fname in old_migrations:
        create_migration_file(migrations_dir, fname, sql="SELECT 1;")
    changelog = create_empty_changelog(temp_dir)

    changelog.migrations.append(Migration(name=old_migrations[0], parents=[INIT_MIGRATION]))
    changelog.migrations.append(Migration(name=old_migrations[1], parents=[old_migrations[0]]))
    client.config.changelog = changelog

    for migration in changelog.migrations:
        client.apply_migration(migration, is_fake=False)

    create_migration_file(migrations_dir, new_migration_name, sql="-- squashed content")
    new_migration = Migration(name=new_migration_name, parents=old_migrations)

    client.squash_migrations(migrations=old_migrations, new_migration=new_migration)

    with client.connection.cursor() as cursor:
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
