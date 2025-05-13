import os
import shutil
import sys
import tempfile
import unittest
from io import StringIO

import psycopg2

from migrateit.clients import PsqlClient
from migrateit.files import create_migrations_file
from migrateit.models import MigrateItConfig, Migration, MigrationsFile


class TestPsqlClientApplyMigrations(unittest.TestCase):
    TEST_MIGRATIONS_TABLE = "migrations"
    TEST_TABLE = "test_entity"

    def setUp(self):
        self._original_stdout = sys.stdout
        sys.stdout = StringIO()

        self.temp_dir = tempfile.mkdtemp()
        self.migrations_dir = os.path.join(self.temp_dir, "migrations")
        self.migrations_file = os.path.join(self.temp_dir, "changelog.json")

        os.makedirs(self.migrations_dir)
        create_migrations_file(self.migrations_file)

        self.connection = psycopg2.connect(PsqlClient.get_environment_url())
        self.config = MigrateItConfig(
            table_name=self.TEST_MIGRATIONS_TABLE,
            migrations_dir=self.migrations_dir,
            migrations_file=self.migrations_file,
        )
        self.client = PsqlClient(connection=self.connection, config=self.config)

        self.client.create_migrations_table()

    def tearDown(self):
        sys.stdout = self._original_stdout
        self._drop_test_table()
        self.connection.close()
        shutil.rmtree(self.temp_dir)

    def _drop_test_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEST_MIGRATIONS_TABLE}")
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEST_TABLE}")
        self.connection.commit()

    def _create_empty_changelog(self) -> MigrationsFile:
        return MigrationsFile(version=1, migrations=[])

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

        self.client.apply_migration(changelog, migration)

        # Check it was inserted into the table
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {self.TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
            result = cursor.fetchone()
            self.assertEqual(result[0] if result else None, 1)

    def test_apply_migration_file_missing(self):
        changelog = self._create_empty_changelog()
        migration = Migration(name="not_found.sql")

        with self.assertRaises(AssertionError):
            self.client.apply_migration(changelog, migration)

    def test_apply_migration_already_applied(self):
        filename = "0001_applied.sql"
        self._create_migrations_dir_and_file(filename, sql="SELECT 1;")
        changelog = self._create_empty_changelog()
        migration = Migration(name=filename)
        changelog.migrations.append(migration)

        self.client.apply_migration(changelog, migration)

        # Second time should raise assertion
        with self.assertRaises(AssertionError):
            self.client.apply_migration(changelog, migration)

    def test_apply_migration_wrong_extension(self):
        filename = "0002_wrong_ext.txt"
        self._create_migrations_dir_and_file(filename, sql="SELECT 1;")
        changelog = self._create_empty_changelog()
        migration = Migration(name=filename)

        with self.assertRaises(AssertionError):
            self.client.apply_migration(changelog, migration)
