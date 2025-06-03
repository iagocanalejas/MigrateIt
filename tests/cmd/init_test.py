import os
from unittest.mock import patch

from migrateit.cli import cmd_init
from migrateit.models.changelog import SupportedDatabase
from migrateit.tree import ROLLBACK_SPLIT_TAG
from tests.cmd._base_test import BaseCmdTest


@patch("migrateit.reporters.output.write_line_b", lambda *_: None)
@patch("migrateit.clients.psql.PsqlClient.create_migrations_table_str", lambda **_: ("-- create", "-- drop"))
class CliInitTest(BaseCmdTest):
    def test_cmd_init(self):
        cmd_init(
            table_name=self.TEST_MIGRATIONS_TABLE,
            migrations_dir=self.migrations_dir,
            migrations_file=self.temp_dir / "changelog.json",
            database=SupportedDatabase.POSTGRES,
        )

        self.assertTrue(os.path.exists(self.migrations_dir))
        self.assertTrue(os.path.exists(self.temp_dir / "changelog.json"))
        self.assertTrue(os.path.exists(self.migrations_dir / "0000_migrateit.sql"))

        content = (self.migrations_dir / "0000_migrateit.sql").read_text()
        self.assertIn("-- create", content)
        self.assertIn("-- drop", content)
        self.assertIn(ROLLBACK_SPLIT_TAG, content)

    def test_cmd_init_missing_rollback_tag(self):
        # Write invalid migration content before calling
        path = self.migrations_dir / "0000_migrateit.sql"
        os.makedirs(self.migrations_dir, exist_ok=True)
        path.write_text("-- Missing rollback tag")

        with self.assertRaises(FileExistsError) as ctx:
            cmd_init(
                table_name=self.TEST_MIGRATIONS_TABLE,
                migrations_dir=self.migrations_dir,
                migrations_file=self.temp_dir / "changelog.json",
                database=SupportedDatabase.POSTGRES,
            )
        self.assertIn("already exists", str(ctx.exception))
