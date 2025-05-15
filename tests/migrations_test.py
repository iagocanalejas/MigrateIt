import os
import shutil
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path

from migrateit.models import ChangelogFile, Migration, SupportedDatabase


class TestMigrationFileUtils(unittest.TestCase):
    def setUp(self):
        self._original_stdout = sys.stdout
        sys.stdout = StringIO()

        self.temp_dir = Path(tempfile.mkdtemp())
        self.migrations_dir = self.temp_dir / "migrations"
        self.migrations_file_path = self.temp_dir / "changelog.json"

    def tearDown(self):
        sys.stdout = self._original_stdout
        shutil.rmtree(self.temp_dir)

    def test_create_migrations_dir_success(self):
        Migration.create_directory(self.migrations_dir)
        self.assertTrue(os.path.exists(self.migrations_dir))

    def test_create_migrations_dir_already_exists(self):
        os.makedirs(self.migrations_dir)
        Migration.create_directory(self.migrations_dir)
        self.assertTrue(os.path.exists(self.migrations_dir))

    def test_create_migrations_file_success(self):
        ChangelogFile.create_file(self.migrations_file_path, SupportedDatabase.POSTGRES)
        self.assertTrue(os.path.exists(self.migrations_file_path))

    def test_create_migrations_file_already_exists(self):
        Path(self.migrations_file_path).touch()
        with self.assertRaises(AssertionError):
            ChangelogFile.create_file(self.migrations_file_path, SupportedDatabase.POSTGRES)

    def test_create_migrations_file_invalid_extension(self):
        bad_path = self.temp_dir / "migrations.txt"
        with self.assertRaises(AssertionError):
            ChangelogFile.create_file(bad_path, SupportedDatabase.POSTGRES)

    def test_load_migrations_file_success(self):
        file = ChangelogFile(version=1, path=self.migrations_file_path)
        with open(self.migrations_file_path, "w") as f:
            f.write(file.to_json())

        loaded = ChangelogFile.load_file(self.migrations_file_path)
        self.assertIsInstance(loaded, ChangelogFile)
        self.assertEqual(loaded.version, 1)

    def test_load_migrations_file_not_exists(self):
        with self.assertRaises(AssertionError):
            ChangelogFile.load_file(self.migrations_file_path)

    def test_save_migrations_file_success(self):
        file = ChangelogFile(version=2, path=self.migrations_file_path)
        Path(self.migrations_file_path).touch()
        file.save_file()

        with open(self.migrations_file_path) as f:
            content = f.read()
        self.assertIn('"version": 2', content)

    def test_save_migrations_file_not_exists(self):
        file = ChangelogFile(version=1, path=self.migrations_file_path)
        with self.assertRaises(AssertionError):
            file.save_file()

    def test_create_new_migration_success(self):
        os.makedirs(self.migrations_dir)
        ChangelogFile.create_file(self.migrations_file_path, SupportedDatabase.POSTGRES)
        changelog = ChangelogFile.load_file(self.migrations_file_path)

        Migration.create_new(changelog, self.migrations_dir, "init")
        created_files = os.listdir(self.migrations_dir)

        self.assertEqual(len(created_files), 1)
        self.assertRegex(created_files[0], r"0000_init\.sql")

        migrations = ChangelogFile.load_file(self.migrations_file_path)
        self.assertEqual(len(migrations.migrations), 1)
        self.assertTrue(migrations.migrations[0].name.endswith("init.sql"))

    def test_create_new_migration_invalid_name(self):
        os.makedirs(self.migrations_dir)
        ChangelogFile.create_file(self.migrations_file_path, SupportedDatabase.POSTGRES)
        changelog = ChangelogFile.load_file(self.migrations_file_path)

        with self.assertRaises(AssertionError):
            Migration.create_new(changelog, self.migrations_dir, "123-bad-name")

        with self.assertRaises(AssertionError):
            Migration.create_new(changelog, self.migrations_dir, "")
