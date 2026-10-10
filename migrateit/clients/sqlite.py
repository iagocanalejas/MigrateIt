import os
import re
from functools import partial
from typing import TYPE_CHECKING, Any, override

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.clients._protocol import ExportItem

if TYPE_CHECKING:
    import sqlite3  # noqa: F401


class SqliteClient(SqlClient["sqlite3.Connection"]):
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
        cursor = self.connection.execute(query, params)
        if any(m in query.upper() for m in ("SELECT", "SHOW", "DESCRIBE")):
            cursor.fetchall()

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

    @property
    @override
    def export_items(self) -> list[ExportItem]:
        return [
            ExportItem(
                name="tables",
                metadata_query="""
SELECT name, sql
FROM sqlite_schema
WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
ORDER BY name;
                """,
                process_rows=partial(_process_tables, self),
            ),
            ExportItem(
                name="views",
                metadata_query="""
SELECT name, sql
FROM sqlite_schema
WHERE type = 'view' AND name NOT LIKE 'sqlite_%'
ORDER BY name;
                """,
                process_rows=partial(_process_views, self),
            ),
            ExportItem(
                name="indexes",
                metadata_query="""
SELECT name, tbl_name, sql
FROM sqlite_schema
WHERE type = 'index' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
ORDER BY name;
                """,
                process_rows=partial(_process_indexes, self),
            ),
            ExportItem(
                name="triggers",
                metadata_query="""
SELECT name, tbl_name, sql
FROM sqlite_schema
WHERE type = 'trigger' AND name NOT LIKE 'sqlite_%'
ORDER BY name;
                """,
                process_rows=partial(_process_triggers, self),
            ),
        ]


# ---------------------------------------------------------------------------
# Module-level row processors
# ---------------------------------------------------------------------------


def _ensure_if_not_exists(sql: str, prefix: str) -> str:
    """Add IF NOT EXISTS to a CREATE statement if not already present."""
    if prefix not in sql.upper():
        return re.sub(rf"(?i){re.escape(prefix)}", prefix + "IF NOT EXISTS ", sql, count=1)
    return sql


def _process_tables(client: SqliteClient, rows: list[tuple[str, str]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for name, sql in rows:
        if name.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        sql_str = sql.strip()
        if not sql_str.endswith(";"):  # pragma: no cover[safety]
            sql_str += ";"
        fwd.append(_ensure_if_not_exists(sql_str, "CREATE TABLE "))
        rb.append(f"DROP TABLE IF EXISTS {client._q(name)};")
    return fwd, list(reversed(rb))


def _process_views(client: SqliteClient, rows: list[tuple[str, str]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for name, sql in rows:
        sql_str = sql.strip()
        if not sql_str.endswith(";"):  # pragma: no cover[safety]
            sql_str += ";"
        fwd.append(_ensure_if_not_exists(sql_str, "CREATE VIEW "))
        rb.append(f"DROP VIEW IF EXISTS {client._q(name)};")
    return fwd, rb


def _process_indexes(client: SqliteClient, rows: list[tuple[str, str, str]]) -> tuple[list[str], list[str]]:
    fwd = []
    for _name, tbl_name, sql in rows:
        if tbl_name.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        sql_str = sql.strip()
        if not sql_str.endswith(";"):  # pragma: no cover[safety]
            sql_str += ";"

        sql_str = _ensure_if_not_exists(sql_str, "CREATE UNIQUE INDEX ")
        sql_str = _ensure_if_not_exists(sql_str, "CREATE INDEX ")
        fwd.append(sql_str)
    # Rollback omitted: SQLite automatically drops indexes when the table is dropped.
    return fwd, []


def _process_triggers(client: SqliteClient, rows: list[tuple[str, str, str]]) -> tuple[list[str], list[str]]:
    fwd = []
    for _name, tbl_name, sql in rows:
        if tbl_name.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        sql_str = sql.strip()
        if not sql_str.endswith(";"):  # pragma: no cover[safety]
            sql_str += ";"

        fwd.append(_ensure_if_not_exists(sql_str, "CREATE TRIGGER "))
    # Rollback omitted: SQLite automatically drops triggers when the table is dropped.
    return fwd, []
