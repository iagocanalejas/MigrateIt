import os
from unittest.mock import patch

from migrateit.cli import cmd_init, cmd_new
from migrateit.clients.psql import PsqlClient
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.config import MigrateItConfig
from migrateit.tree import load_changelog_file
from tests.cmd._base_test import BaseCmdTest


@patch("migrateit.reporters.output.write_line_b", lambda *_: None)
class CliNewTest(BaseCmdTest):
    def setUp(self):
        super().setUp()

        with patch("migrateit.reporters.output.write_line_b", lambda *_: None):
            cmd_init(
                table_name=self.TEST_MIGRATIONS_TABLE,
                migrations_dir=self.migrations_dir,
                migrations_file=self.temp_dir / "changelog.json",
                database=SupportedDatabase.POSTGRES,
            )

        self.changelog = load_changelog_file(self.temp_dir / "changelog.json")
        self.config = MigrateItConfig(
            table_name=self.TEST_MIGRATIONS_TABLE,
            migrations_dir=self.migrations_dir,
            changelog=self.changelog,
        )
        self.client = PsqlClient(connection=self.connection, config=self.config)

    def test_cmd_new(self):
        cmd_new(
            client=self.client,
            name="test_migration",
            no_edit=True,
        )

        self.assertTrue(os.path.exists(self.migrations_dir / "0001_test_migration.sql"))

        changelog = load_changelog_file(self.temp_dir / "changelog.json")
        self.assertEqual(len(changelog.migrations), 2)
        self.assertEqual(changelog.migrations[1].name, "0001_test_migration.sql")

    def test_cmd_new_with_existing_migration(self):
        with open(self.migrations_dir / "0001_test_migration.sql", "w", encoding="utf-8") as f:
            f.write("Hello, world!\n")

        with self.assertRaises(FileExistsError) as ctx:
            cmd_new(
                client=self.client,
                name="test_migration",
                no_edit=True,
            )

        self.assertIn("already exists", str(ctx.exception))

    def test_cmd_new_with_dependencies(self):
        cmd_new(
            client=self.client,
            name="test_migration",
            no_edit=True,
        )
        cmd_new(
            client=self.client,
            name="test_migration",
            dependencies=["0000", "0001"],
            no_edit=True,
        )

        self.assertTrue(os.path.exists(self.migrations_dir / "0002_test_migration.sql"))

        changelog = load_changelog_file(self.temp_dir / "changelog.json")
        self.assertEqual(len(changelog.migrations), 3)
        self.assertEqual(changelog.migrations[2].name, "0002_test_migration.sql")
        self.assertEqual(changelog.migrations[2].parents, ["0000_migrateit.sql", "0001_test_migration.sql"])
