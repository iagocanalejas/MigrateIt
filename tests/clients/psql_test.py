import sys
import unittest
from io import StringIO

import psycopg2

from migrateit.clients import PsqlClient
from migrateit.models import MigrateItConfig


class TestPsqlClient(unittest.TestCase):
    TEST_TABLE = "migrations"
    TEST_MIGRATIONS_DIR = "migrations"
    TEST_MIGRATIONS_FILE = "changelog.json"

    def setUp(self):
        self._original_stdout = sys.stdout
        sys.stdout = StringIO()

        self.connection = psycopg2.connect(PsqlClient.get_environment_url())
        self.config = MigrateItConfig(
            table_name=self.TEST_TABLE,
            migrations_dir=self.TEST_MIGRATIONS_DIR,
            migrations_file=self.TEST_MIGRATIONS_FILE,
        )
        self.client = PsqlClient(connection=self.connection, config=self.config)
        self._drop_test_table()  # ensure clean state

    def tearDown(self):
        sys.stdout = self._original_stdout
        self._drop_test_table()
        self.connection.close()

    def _drop_test_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEST_TABLE}")
        self.connection.commit()

    def test_check_migrations_table_exist_false(self):
        self.assertFalse(self.client.check_migrations_table_exist())

    def test_create_and_check_table(self):
        self.client.create_migrations_table()
        self.assertTrue(self.client.check_migrations_table_exist())

    def test_create_table_twice_fails(self):
        self.client.create_migrations_table()
        with self.assertRaises(AssertionError):
            self.client.create_migrations_table()
