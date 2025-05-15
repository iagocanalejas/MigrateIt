import os

from tests.clients.psql._base_test import BasePsqlTest

from migrateit.models import ChangelogFile, Migration


class TestPsqlClientApplyMigrations(BasePsqlTest):
    TEST_TABLE = "test_entity"

    def setUp(self):
        super().setUp()

        os.makedirs(self.migrations_dir)
        self.client.create_migrations_table()

    def tearDown(self):
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEST_TABLE}")
        self.connection.commit()
        super().tearDown()

    def _create_empty_changelog(self) -> ChangelogFile:
        return ChangelogFile(version=1, migrations=[])

    def _create_migrations_dir_and_file(self, filename: str, sql: str | None = None):
        path = os.path.join(self.migrations_dir, filename)
        with open(path, "w") as f:
            f.write(sql or f"-- Migration {filename}")
        return path

    def test_apply_migration_success(self):
        filename = "0000_init.sql"
        self._create_migrations_dir_and_file(
            filename,
            sql=f"""
            CREATE TABLE IF NOT EXISTS {self.TEST_TABLE} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
        )
        changelog = self._create_empty_changelog()
        migration = Migration(name=filename)
        changelog.migrations.append(migration)
        self.client.config.changelog = changelog

        self.client.apply_migration(migration)

        # Check it was inserted into the table
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {self.TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
            result = cursor.fetchone()
            self.assertEqual(result[0] if result else None, 1)

    def test_apply_migration_file_missing(self):
        self.client.config.changelog = self._create_empty_changelog()
        migration = Migration(name="not_found.sql")

        with self.assertRaises(AssertionError):
            self.client.apply_migration(migration)

    def test_apply_migration_already_applied(self):
        filename = "0001_applied.sql"
        self._create_migrations_dir_and_file(filename, sql="SELECT 1;")
        changelog = self._create_empty_changelog()
        migration = Migration(name=filename)
        changelog.migrations.append(migration)

        self.client.config.changelog = changelog
        self.client.apply_migration(migration)
        with self.assertRaises(AssertionError):
            self.client.apply_migration(migration)

    def test_apply_migration_wrong_extension(self):
        filename = "0002_wrong_ext.txt"
        self._create_migrations_dir_and_file(filename, sql="SELECT 1;")
        changelog = self._create_empty_changelog()
        migration = Migration(name=filename)

        self.client.config.changelog = changelog
        with self.assertRaises(AssertionError):
            self.client.apply_migration(migration)
