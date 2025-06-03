import os
import shutil
import tempfile
import unittest
from pathlib import Path

import psycopg2

from migrateit.clients.psql import PsqlClient
from migrateit.tree import ROLLBACK_SPLIT_TAG


class BaseCmdTest(unittest.TestCase):
    TEST_MIGRATIONS_TABLE = "migrations"

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.migrations_dir = self.temp_dir / "migrations"

        self.connection = psycopg2.connect(PsqlClient.get_environment_url())
        sql, _ = PsqlClient.create_migrations_table_str(self.TEST_MIGRATIONS_TABLE)
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            self.connection.commit()

    def tearDown(self):
        self._drop_test_table()
        self.connection.close()
        shutil.rmtree(self.temp_dir)

    def _drop_test_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEST_MIGRATIONS_TABLE}")
        self.connection.commit()

    def _create_migrations_file(self, filename: str, sql: str | None = None, rollback_sql: str | None = None) -> str:
        path = os.path.join(self.migrations_dir, filename)
        with open(path, "w") as f:
            f.write(sql or f"-- Migration {filename}\n")
            f.write(f"{ROLLBACK_SPLIT_TAG}")
            if rollback_sql:
                f.write(f"\n\n{rollback_sql}")
        return path
