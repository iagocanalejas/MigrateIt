import shutil
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path

import psycopg2

from migrateit.clients import PsqlClient
from migrateit.models import MigrateItConfig, SupportedDatabase
from migrateit.tree import create_changelog_file


class BasePsqlTest(unittest.TestCase):
    TEST_MIGRATIONS_TABLE = "migrations"

    def setUp(self):
        self._original_stdout = sys.stdout
        sys.stdout = StringIO()

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
        sys.stdout = self._original_stdout
        self._drop_test_table()
        self.connection.close()
        shutil.rmtree(self.temp_dir)

    def _drop_test_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEST_MIGRATIONS_TABLE}")
        self.connection.commit()
