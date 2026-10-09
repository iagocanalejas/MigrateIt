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
                process_row=partial(_process_tables, self),
            ),
            ExportItem(
                name="tables_emit",
                metadata_query="SELECT 1;",
                process_row=partial(_emit_tables, self),
            ),
            ExportItem(
                name="views",
                metadata_query="""
SELECT name, sql
FROM sqlite_schema
WHERE type = 'view' AND name NOT LIKE 'sqlite_%'
ORDER BY name;
                """,
                process_row=partial(_process_views, self),
            ),
            ExportItem(
                name="indexes",
                metadata_query="""
SELECT name, tbl_name, sql
FROM sqlite_schema
WHERE type = 'index' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
ORDER BY name;
                """,
                process_row=partial(_process_indexes, self),
            ),
            ExportItem(
                name="triggers",
                metadata_query="""
SELECT name, tbl_name, sql
FROM sqlite_schema
WHERE type = 'trigger' AND name NOT LIKE 'sqlite_%'
ORDER BY name;
                """,
                process_row=partial(_process_triggers, self),
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


def _process_tables(client: SqliteClient, row: tuple[str, str]) -> tuple[list[str], list[str]]:
    name, sql = row
    if name.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
        return [], []

    sql_str = sql.strip()
    if not sql_str.endswith(";"):  # pragma: no cover[safety]
        sql_str += ";"

    sql_str = _ensure_if_not_exists(sql_str, "CREATE TABLE ")

    # Accumulate columns per table using a mutable side-channel on the client
    if not hasattr(client, "_export_columns_fwr"):
        client._export_columns_fwr = []  # type: ignore[attr-defined]
    if not hasattr(client, "_export_columns_rb"):
        client._export_columns_rb = []  # type: ignore[attr-defined]
    client._export_columns_fwr.append(sql_str)  # type: ignore[attr-defined]
    client._export_columns_rb.append(f"DROP TABLE IF EXISTS {client._q(name)};")  # type: ignore[attr-defined]
    return [], []


def _emit_tables(client: SqliteClient, _row: Any) -> tuple[list[str], list[str]]:
    fwr: list[str] = getattr(client, "_export_columns_fwr", [])
    rb: list[str] = getattr(client, "_export_columns_rb", [])
    return fwr, list(reversed(rb))


def _process_views(client: SqliteClient, row: tuple[str, str]) -> tuple[list[str], list[str]]:
    name, sql = row

    sql_str = sql.strip()
    if not sql_str.endswith(";"):  # pragma: no cover[safety]
        sql_str += ";"

    sql_str = _ensure_if_not_exists(sql_str, "CREATE VIEW ")

    fwd = [sql_str]
    rb = [f"DROP VIEW IF EXISTS {client._q(name)};"]
    return fwd, rb


def _process_indexes(client: SqliteClient, row: tuple[str, str, str]) -> tuple[list[str], list[str]]:
    _name, tbl_name, sql = row
    if tbl_name.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
        return [], []

    sql_str = sql.strip()
    if not sql_str.endswith(";"):  # pragma: no cover[safety]
        sql_str += ";"

    sql_str = _ensure_if_not_exists(sql_str, "CREATE UNIQUE INDEX ")
    sql_str = _ensure_if_not_exists(sql_str, "CREATE INDEX ")

    # Rollback omitted: SQLite automatically drops indexes when the table is dropped.
    return [sql_str], []


def _process_triggers(client: SqliteClient, row: tuple[str, str, str]) -> tuple[list[str], list[str]]:
    _name, tbl_name, sql = row
    if tbl_name.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
        return [], []

    sql_str = sql.strip()
    if not sql_str.endswith(";"):  # pragma: no cover[safety]
        sql_str += ";"

    sql_str = _ensure_if_not_exists(sql_str, "CREATE TRIGGER ")

    # Rollback omitted: SQLite automatically drops triggers when the table is dropped.
    return [sql_str], []
