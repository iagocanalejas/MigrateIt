import os
import re
import sqlite3
from typing import Any, override

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.models.migration import Migration, get_migration_header
from migrateit.reporters.output import write_line


class SqliteClient(SqlClient[sqlite3.Connection]):
    @property
    @override
    def placeholder(self) -> str:
        return "?"

    @override
    @classmethod
    def get_connection_params(cls) -> dict[str, Any]:
        db_url = os.getenv(cls.VARNAME_DB_URL)
        if db_url:
            return {"url": db_url}
        return {"file_name": os.getenv(cls.VARNAME_DB_FILE, "migrateit.db")}

    @override
    @classmethod
    def create_migrations_table_str(cls, table_name: str) -> tuple[str, str]:
        """Create SQLite DDL for the migrations table."""
        if not table_name.isidentifier():
            raise ValueError(f"Unsafe table name: {table_name}")
        migrations_query = f"""
CREATE TABLE IF NOT EXISTS {cls._q(table_name)} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_name VARCHAR(255) UNIQUE NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_hash VARCHAR(64) NOT NULL,
    squashed BOOLEAN DEFAULT 0
);
        """
        reverse_query = f"""
DROP TABLE IF EXISTS {cls._q(table_name)};
        """
        return migrations_query, reverse_query

    @override
    def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        self.connection.execute(query, params)

    @override
    def execute_for_one(self, query: str, params: tuple[Any, ...] = ()) -> Any:
        cursor = self.connection.execute(query, params)
        return cursor.fetchone()

    @override
    def execute_for_rows(self, query: str, params: tuple[Any, ...] = ()) -> list[Any]:
        cursor = self.connection.execute(query, params)
        return cursor.fetchall()

    @override
    def is_migrations_table_created(self) -> bool:
        """Check if the migrations table exists in SQLite."""
        query = """
SELECT name
FROM sqlite_master
WHERE type='table' AND name=?;
"""
        cursor = self.connection.execute(query, (self.table_name,))
        return cursor.fetchone() is not None

    @override
    def export_database_schema(self, migration: Migration) -> None:
        if len(migration.parents) != 1 or migration.parents[0] != self.changelog.root.name:
            raise ValueError("Full database export must depend only on the initial migration")

        forward_ddl = []
        rollback_ddl = []
        write_line(f"Exporting full database schema to '{migration.name}'...")

        # -------------------------------------------------------------
        # 1. TABLES
        # -------------------------------------------------------------
        write_line("\tExporting tables...")
        cursor = self.connection.execute("""
SELECT name, sql
FROM sqlite_schema
WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
ORDER BY name;
        """)
        tables_list = []
        for name, sql in cursor.fetchall():
            if name.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                continue

            sql_str = sql.strip()
            if not sql_str.endswith(";"):  # pragma: no cover[safety]
                sql_str += ";"

            # Ensure idempotent execution with IF NOT EXISTS
            if "CREATE TABLE IF NOT EXISTS" not in sql_str.upper():  # pragma: no cover[safety]
                sql_str = re.sub(
                    r"(?i)^CREATE\s+TABLE\s+",
                    "CREATE TABLE IF NOT EXISTS ",
                    sql_str,
                    count=1,
                )

            forward_ddl.append(sql_str)
            tables_list.append(name)

        # Rollback tables in reverse order
        for name in reversed(tables_list):
            rollback_ddl.append(f"DROP TABLE IF EXISTS {self._q(name)};")

        # -------------------------------------------------------------
        # 2. VIEWS
        # -------------------------------------------------------------
        write_line("\tExporting views...")
        cursor = self.connection.execute("""
SELECT name, sql
FROM sqlite_schema
WHERE type = 'view' AND name NOT LIKE 'sqlite_%'
ORDER BY name;
        """)
        views_list = []
        for name, sql in cursor.fetchall():
            sql_str = sql.strip()
            if not sql_str.endswith(";"):  # pragma: no cover[safety]
                sql_str += ";"

            if "CREATE VIEW IF NOT EXISTS" not in sql_str.upper():  # pragma: no cover[safety]
                sql_str = re.sub(
                    r"(?i)^CREATE\s+VIEW\s+",
                    "CREATE VIEW IF NOT EXISTS ",
                    sql_str,
                    count=1,
                )

            forward_ddl.append(sql_str)
            views_list.append(name)

        for name in reversed(views_list):
            rollback_ddl.append(f"DROP VIEW IF EXISTS {self._q(name)};")

        # -------------------------------------------------------------
        # 3. INDEXES
        # (Filtering sql IS NOT NULL skips auto-generated PRIMARY KEY / UNIQUE indexes)
        # -------------------------------------------------------------
        write_line("\tExporting indexes...")
        cursor = self.connection.execute("""
SELECT name, tbl_name, sql
FROM sqlite_schema
WHERE type = 'index' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
ORDER BY name;
        """)
        for name, tbl_name, sql in cursor.fetchall():
            if tbl_name.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                continue

            sql_str = sql.strip()
            if not sql_str.endswith(";"):  # pragma: no cover[safety]
                sql_str += ";"

            if (
                "CREATE INDEX IF NOT EXISTS" not in sql_str.upper()
                and "CREATE UNIQUE INDEX IF NOT EXISTS" not in sql_str.upper()
            ):  # pragma: no cover[safety]
                sql_str = re.sub(
                    r"(?i)^CREATE\s+(UNIQUE\s+)?INDEX\s+",
                    r"CREATE \1INDEX IF NOT EXISTS ",
                    sql_str,
                    count=1,
                )

            forward_ddl.append(sql_str)
            # Rollback omitted: SQLite automatically drops indexes when the table is dropped.

        # -------------------------------------------------------------
        # 4. TRIGGERS
        # -------------------------------------------------------------
        write_line("\tExporting triggers...")
        cursor = self.connection.execute("""
SELECT name, tbl_name, sql
FROM sqlite_schema
WHERE type = 'trigger' AND name NOT LIKE 'sqlite_%'
ORDER BY name;
        """)
        for name, tbl_name, sql in cursor.fetchall():
            if tbl_name.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                continue

            sql_str = sql.strip()
            if not sql_str.endswith(";"):  # pragma: no cover[safety]
                sql_str += ";"

            if "CREATE TRIGGER IF NOT EXISTS" not in sql_str.upper():  # pragma: no cover[safety]
                sql_str = re.sub(
                    r"(?i)^CREATE\s+TRIGGER\s+",
                    "CREATE TRIGGER IF NOT EXISTS ",
                    sql_str,
                    count=1,
                )

            forward_ddl.append(sql_str)
            # Rollback omitted: SQLite automatically drops triggers when the table is dropped.

        migration_path = self.migrations_dir / migration.name
        with open(migration_path, "w", encoding="utf-8") as f:
            f.write(get_migration_header(migration_path))
            f.write("-- Migration automatically generated by migrateit\n\n")
            f.write("\n\n".join(forward_ddl) + "\n\n\n")
            f.write(C.ROLLBACK_SPLIT_TAG + "\n\n\n")
            f.write("\n\n".join(reversed(rollback_ddl)) + "\n")
