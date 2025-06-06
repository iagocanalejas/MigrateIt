import os

from migrateit.models import Migration
from tests.clients.psql._base_test import BasePsqlTest


class TestPsqlClientSquashMigrations(BasePsqlTest):
    TEST_TABLE = "test_entity"

    def setUp(self):
        super().setUp()

        os.makedirs(self.migrations_dir)
        sql, _ = self.client.create_migrations_table_str(self.TEST_MIGRATIONS_TABLE)
        self._create_migrations_file(self.INIT_MIGRATION, sql=sql)

        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            self.connection.commit()

    def test_squash_migrations_marks_old_as_squashed_and_applies_new_fake(self):
        old_migrations = ["0010_one.sql", "0011_two.sql"]
        new_migration_name = "0012_squashed.sql"

        for fname in old_migrations:
            self._create_migrations_file(fname, sql="SELECT 1;")
        changelog = self._create_empty_changelog()

        changelog.migrations.append(Migration(name=old_migrations[0], parents=[self.INIT_MIGRATION]))
        changelog.migrations.append(Migration(name=old_migrations[1], parents=[old_migrations[0]]))
        self.client.config.changelog = changelog

        for migration in changelog.migrations:
            self.client.apply_migration(migration, is_fake=False)

        self._create_migrations_file(new_migration_name, sql="-- squashed content")
        new_migration = Migration(name=new_migration_name, parents=old_migrations)

        self.client.squash_migrations(migrations=old_migrations, new_migration=new_migration)

        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""SELECT COUNT(*) FROM {self.TEST_MIGRATIONS_TABLE}
                    WHERE migration_name IN %s AND squashed = TRUE""",
                (tuple(old_migrations),),
            )
            result = cursor.fetchone()
            self.assertEqual(result[0] if result else None, len(old_migrations))

            # Assert new migration is recorded
            cursor.execute(
                f"SELECT COUNT(*) FROM {self.TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (new_migration_name,)
            )
            result = cursor.fetchone()
            self.assertEqual(result[0] if result else None, 1)
