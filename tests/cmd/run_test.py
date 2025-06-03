from unittest.mock import patch

import psycopg2

from migrateit.cli import cmd_init, cmd_new, cmd_run
from migrateit.clients.psql import PsqlClient
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.config import MigrateItConfig
from migrateit.tree import load_changelog_file
from tests.cmd._base_test import BaseCmdTest


@patch("migrateit.reporters.output.write_line_b", lambda *_: None)
class CliRunTest(BaseCmdTest):
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

    def test_cmd_run_and_rerun(self):
        cmd_new(self.client, name="new", no_edit=True)
        self._create_migrations_file("0001_new.sql", sql="SELECT 1;")

        cmd_run(client=self.client)
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.TEST_MIGRATIONS_TABLE}")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 2)

        cmd_run(client=self.client)
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.TEST_MIGRATIONS_TABLE}")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 2)

    def test_cmd_run_by_name(self):
        cmd_new(self.client, name="new", no_edit=True)
        self._create_migrations_file("0001_new.sql", sql="SELECT 1;")

        cmd_run(client=self.client, name="0001")
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.TEST_MIGRATIONS_TABLE}")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 2)

    def test_cmd_run_by_name_not_found(self):
        with self.assertRaises(ValueError) as ctx:
            cmd_run(client=self.client, name="0010")
        self.assertIn("Migration '0010' not found", str(ctx.exception))

    def test_cmd_run_fake(self):
        cmd_new(self.client, name="new", no_edit=True)
        self._create_migrations_file("0001_new.sql", sql="CREATE TABLE test (id serial primary key);")

        cmd_run(client=self.client, name="0001", is_fake=True)
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.TEST_MIGRATIONS_TABLE}")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 1)

            with self.assertRaises(psycopg2.errors.UndefinedTable):
                cursor.execute("SELECT * FROM test")
            self.connection.rollback()

    def test_cmd_run_rollback(self):
        cmd_new(self.client, name="new", no_edit=True)
        self._create_migrations_file(
            "0001_new.sql",
            sql="CREATE TABLE test (id serial primary key);",
            rollback_sql="DROP TABLE test;",
        )

        cmd_run(client=self.client, name="0001")
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.TEST_MIGRATIONS_TABLE}")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 2)
            cursor.execute("SELECT * FROM test")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 0)

        cmd_run(client=self.client, name="0001", is_rollback=True)
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.TEST_MIGRATIONS_TABLE}")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 1)
