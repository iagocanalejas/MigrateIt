import os
import shutil
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path

from migrateit.files import (
    create_migrations_dir,
    create_migrations_file,
    create_new_migration,
    load_migrations_file,
    save_migrations_file,
)
from migrateit.models import MigrateItConfig, MigrationsFile


class TestMigrationFileUtils(unittest.TestCase):
    def setUp(self):
        self._original_stdout = sys.stdout
        sys.stdout = StringIO()

        self.temp_dir = tempfile.mkdtemp()
        self.migrations_dir = os.path.join(self.temp_dir, "migrations")
        self.migrations_file_path = os.path.join(self.temp_dir, "changelog.json")
        self.config = MigrateItConfig(
            table_name="",
            migrations_dir=self.migrations_dir,
            migrations_file=self.migrations_file_path,
        )

    def tearDown(self):
        sys.stdout = self._original_stdout
        shutil.rmtree(self.temp_dir)

    def test_create_migrations_dir_success(self):
        create_migrations_dir(self.migrations_dir)
        self.assertTrue(os.path.exists(self.migrations_dir))

    def test_create_migrations_dir_already_exists(self):
        os.makedirs(self.migrations_dir)
        with self.assertRaises(AssertionError):
            create_migrations_dir(self.migrations_dir)

    def test_create_migrations_file_success(self):
        create_migrations_file(self.migrations_file_path)
        self.assertTrue(os.path.exists(self.migrations_file_path))

    def test_create_migrations_file_already_exists(self):
        Path(self.migrations_file_path).touch()
        with self.assertRaises(AssertionError):
            create_migrations_file(self.migrations_file_path)

    def test_create_migrations_file_invalid_extension(self):
        bad_path = os.path.join(self.temp_dir, "migrations.txt")
        with self.assertRaises(AssertionError):
            create_migrations_file(bad_path)

    def test_load_migrations_file_success(self):
        file = MigrationsFile(version=1)
        with open(self.migrations_file_path, "w") as f:
            f.write(file.to_json())

        loaded = load_migrations_file(self.migrations_file_path)
        self.assertIsInstance(loaded, MigrationsFile)
        self.assertEqual(loaded.version, 1)

    def test_load_migrations_file_not_exists(self):
        with self.assertRaises(AssertionError):
            load_migrations_file(self.migrations_file_path)

    def test_save_migrations_file_success(self):
        file = MigrationsFile(version=2)
        Path(self.migrations_file_path).touch()
        save_migrations_file(self.migrations_file_path, file)

        with open(self.migrations_file_path) as f:
            content = f.read()
        self.assertIn('"version": 2', content)

    def test_save_migrations_file_not_exists(self):
        file = MigrationsFile(version=1)
        with self.assertRaises(AssertionError):
            save_migrations_file(self.migrations_file_path, file)

    def test_create_new_migration_success(self):
        os.makedirs(self.migrations_dir)
        create_migrations_file(self.migrations_file_path)

        create_new_migration(self.config, "init")
        created_files = os.listdir(self.migrations_dir)

        self.assertEqual(len(created_files), 1)
        self.assertRegex(created_files[0], r"0000_init\.sql")

        migrations = load_migrations_file(self.migrations_file_path)
        self.assertEqual(len(migrations.migrations), 1)
        self.assertTrue(migrations.migrations[0].name.endswith("init.sql"))

    def test_create_new_migration_invalid_name(self):
        os.makedirs(self.migrations_dir)
        create_migrations_file(self.migrations_file_path)

        with self.assertRaises(AssertionError):
            create_new_migration(self.config, "123-bad-name")

        with self.assertRaises(AssertionError):
            create_new_migration(self.config, "")
