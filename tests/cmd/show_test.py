from unittest.mock import patch

from migrateit.cli import cmd_init
from migrateit.clients.psql import PsqlClient
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.config import MigrateItConfig
from migrateit.tree import load_changelog_file
from tests.cmd._base_test import BaseCmdTest


# TODO: check how to test show command
class CliShowTest(BaseCmdTest):
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

    def test_cmd_show(self):
        pass
