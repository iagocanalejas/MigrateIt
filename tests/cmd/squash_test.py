from unittest.mock import patch

from migrateit.cli import cmd_init, cmd_new, cmd_run, cmd_squash
from migrateit.clients.psql import PsqlClient
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.config import MigrateItConfig
from migrateit.tree import load_changelog_file
from tests.cmd._base_test import BaseCmdTest


@patch("migrateit.reporters.output.write_line_b", lambda *_: None)
class CliSquashTest(BaseCmdTest):
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

    def test_cmd_squash_applied(self):
        cmd_new(self.client, name="first", no_edit=True)
        self._create_migrations_file("0001_first.sql", sql="SELECT 1;", rollback_sql="SELECT 1;")
        cmd_new(self.client, name="second", no_edit=True)
        self._create_migrations_file("0002_second.sql", sql="SELECT 2;", rollback_sql="SELECT 2;")

        cmd_run(client=self.client)  # apply both

        result = cmd_squash(client=self.client, start_migration="0001", end_migration="0002", name="squashed_0001_0002")
        self.assertEqual(result, 0)

        changelog_names = [m.name for m in self.client.changelog.migrations]
        self.assertIn("0003_squashed_0001_0002.sql", changelog_names)
        self.assertNotIn("0001_first", changelog_names)
        self.assertNotIn("0002_second", changelog_names)

        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT migration_name FROM {self.TEST_MIGRATIONS_TABLE}")
            applied = {row[0] for row in cursor.fetchall()}
            self.assertIn("0003_squashed_0001_0002.sql", applied)
            self.assertNotIn("0001_first", applied)
            self.assertNotIn("0002_second", applied)
