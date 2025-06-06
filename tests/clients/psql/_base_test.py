import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import psycopg2

from migrateit.clients import PsqlClient
from migrateit.models import MigrateItConfig, SupportedDatabase
from migrateit.models.changelog import ChangelogFile
from migrateit.models.migration import Migration
from migrateit.tree import ROLLBACK_SPLIT_TAG, create_changelog_file


@patch("migrateit.reporters.output.write_line_b", lambda *_: None)
class BasePsqlTest(unittest.TestCase):
    INIT_MIGRATION = "0000_migrateit.sql"
    TEST_MIGRATIONS_TABLE = "migrations"

    def setUp(self):
        self.connection = psycopg2.connect(PsqlClient.get_environment_url())
        self.temp_dir = Path(tempfile.mkdtemp())
        self.migrations_dir = self.temp_dir / "migrations"
        self.changelog = create_changelog_file(self.temp_dir / "changelog.json", SupportedDatabase.POSTGRES)

        self.config = MigrateItConfig(
            table_name=self.TEST_MIGRATIONS_TABLE,
            migrations_dir=self.migrations_dir,
            changelog=self.changelog,
        )
        self.client = PsqlClient(connection=self.connection, config=self.config)
        self._drop_test_table()  # ensure clean state

    def tearDown(self):
        self._drop_test_table()
        self.connection.close()
        shutil.rmtree(self.temp_dir)

    def _drop_test_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEST_MIGRATIONS_TABLE}")
        self.connection.commit()

    def _create_empty_changelog(self) -> ChangelogFile:
        return ChangelogFile(version=1, migrations=[Migration(name=self.INIT_MIGRATION)])

    def _create_migrations_file(self, filename: str, sql: str | None = None, rollback_sql: str | None = None) -> str:
        path = os.path.join(self.migrations_dir, filename)
        with open(path, "w") as f:
            f.write(sql or f"-- Migration {filename}\n")
            f.write(f"{ROLLBACK_SPLIT_TAG}")
            if rollback_sql:
                f.write(f"\n\n{rollback_sql}")
        return path
