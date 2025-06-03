import os

from psycopg2 import ProgrammingError

from migrateit.models.migration import Migration
from tests.clients.psql._base_test import BasePsqlTest


class TestPsqlClientValidation(BasePsqlTest):
    def setUp(self):
        super().setUp()
        os.makedirs(self.migrations_dir)

        sql, _ = self.client.create_migrations_table_str(self.TEST_MIGRATIONS_TABLE)
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            self.connection.commit()

    def test_validate_simple_select_syntax(self):
        filename = "0001_init.sql"
        self._create_migrations_file(filename, sql=f"SELECT * FROM {self.TEST_MIGRATIONS_TABLE};")
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        self.assertIsNone(self.client.validate_sql_syntax(migration))

    def test_validate_simple_select_with_rollback(self):
        filename = "0001_init.sql"
        self._create_migrations_file(
            filename,
            sql=f"SELECT * FROM {self.TEST_MIGRATIONS_TABLE};",
            rollback_sql="SELECT 1;",
        )
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        self.assertIsNone(self.client.validate_sql_syntax(migration))

    def test_validate_create_table_syntax(self):
        filename = "0002_create_table.sql"
        self._create_migrations_file(
            filename,
            sql=f"""
            CREATE TABLE {self.TEST_MIGRATIONS_TABLE}_extra (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
        )
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        self.assertIsNone(self.client.validate_sql_syntax(migration))
        with self.connection.cursor() as cursor:
            with self.assertRaises(ProgrammingError):
                cursor.execute(f"SELECT * FROM {self.TEST_MIGRATIONS_TABLE}_extra;")
        self.connection.rollback()

    def test_invalid_sql_in_migration_code(self):
        filename = "0003_invalid.sql"
        self._create_migrations_file(filename, sql="SELEKT * FRM non_existing_table;")
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        error_result = self.client.validate_sql_syntax(migration)
        self.assertIsInstance(error_result, tuple)
        assert isinstance(error_result, tuple)
        error, sql = error_result
        self.assertIsInstance(error, ProgrammingError)
        self.assertIn("SELEKT", sql)

    def test_invalid_sql_in_rollback_code(self):
        filename = "0004_invalid_rollback.sql"
        self._create_migrations_file(
            filename,
            sql=f"SELECT * FROM {self.TEST_MIGRATIONS_TABLE};",
            rollback_sql="ROLLBAK;",
        )
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        error_result = self.client.validate_sql_syntax(migration)
        self.assertIsInstance(error_result, tuple)
        assert isinstance(error_result, tuple)
        error, sql = error_result
        self.assertIsInstance(error, ProgrammingError)
        self.assertIn("ROLLBAK", sql)

    def test_empty_sql_file_is_skipped(self):
        filename = "0005_empty.sql"
        self._create_migrations_file(filename, sql="")
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        self.assertIsNone(self.client.validate_sql_syntax(migration))

    def test_file_not_found_raises_error(self):
        migration = Migration(name="not_exist.sql", parents=["0000_migrateit.sql"])
        with self.assertRaises(FileNotFoundError):
            self.client.validate_sql_syntax(migration)

    def test_non_sql_file_raises_error(self):
        filename = "0006_script.txt"
        path = self.migrations_dir / filename
        path.write_text("SELECT 1;")
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        with self.assertRaises(FileNotFoundError):
            self.client.validate_sql_syntax(migration)

    def test_validate_multiple_statements(self):
        filename = "0007_multi.sql"
        self._create_migrations_file(
            filename,
            sql=f"""
            INSERT INTO {self.TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES ('1', 'hash1');
            INSERT INTO {self.TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES ('2', 'hash2');
        """,
        )
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        self.assertIsNone(self.client.validate_sql_syntax(migration))

    def test_validate_drop_table_statement(self):
        filename = "0008_drop_table.sql"
        self._create_migrations_file(filename, sql=f"DROP TABLE IF EXISTS {self.TEST_MIGRATIONS_TABLE}_to_drop;")
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        self.assertIsNone(self.client.validate_sql_syntax(migration))

    def test_validate_alter_table_add_column(self):
        # First, create the table
        with self.connection.cursor() as cursor:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.TEST_MIGRATIONS_TABLE}_alter (id INT);")
            self.connection.commit()

        filename = "0009_alter_table_add.sql"
        self._create_migrations_file(
            filename, sql=f"ALTER TABLE {self.TEST_MIGRATIONS_TABLE}_alter ADD COLUMN new_col TEXT;"
        )
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        self.assertIsNone(self.client.validate_sql_syntax(migration))

        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEST_MIGRATIONS_TABLE}_alter;")
            self.connection.commit()

    def test_validate_alter_table_drop_column(self):
        # First, create the table with the column
        with self.connection.cursor() as cursor:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.TEST_MIGRATIONS_TABLE}_alter2 (id INT, to_remove TEXT);")
            self.connection.commit()

        filename = "0010_alter_table_drop.sql"
        self._create_migrations_file(
            filename, sql=f"ALTER TABLE {self.TEST_MIGRATIONS_TABLE}_alter2 DROP COLUMN to_remove;"
        )
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        self.assertIsNone(self.client.validate_sql_syntax(migration))

        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.TEST_MIGRATIONS_TABLE}_alter2;")
            self.connection.commit()

    def test_invalid_drop_table_statement(self):
        filename = "0011_invalid_drop.sql"
        self._create_migrations_file(filename, sql="DROP TABL test_table;")
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        error_result = self.client.validate_sql_syntax(migration)
        self.assertIsInstance(error_result, tuple)
        assert isinstance(error_result, tuple)
        error, sql = error_result
        self.assertIsInstance(error, ProgrammingError)
        self.assertIn("DROP TABL", sql)

    def test_invalid_alter_table_statement(self):
        filename = "0012_invalid_alter.sql"
        self._create_migrations_file(filename, sql="ALTER TABLE some_table ADD COLUM typo_col TEXT;")
        migration = Migration(name=filename, parents=["0000_migrateit.sql"])
        error_result = self.client.validate_sql_syntax(migration)
        self.assertIsInstance(error_result, tuple)
        assert isinstance(error_result, tuple)
        error, sql = error_result
        self.assertIsInstance(error, ProgrammingError)
        self.assertIn("ADD COLUM", sql)
