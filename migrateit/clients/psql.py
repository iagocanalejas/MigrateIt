import os
import re
from collections import defaultdict
from functools import partial
from typing import TYPE_CHECKING, Any, override

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.clients._protocol import ExportItem

if TYPE_CHECKING:
    import psycopg  # noqa: F401


class PsqlClient(SqlClient["psycopg.Connection"]):
    @override
    @classmethod
    def get_connection_params(cls) -> dict[str, Any]:
        db_url = os.getenv(cls.VARNAME_DB_URL)
        if db_url:
            return {"conninfo": db_url}

        return {
            "host": os.getenv(cls.VARNAME_DB_HOST, "localhost"),
            "port": int(os.getenv(cls.VARNAME_DB_PORT, "5432")),
            "user": os.getenv(cls.VARNAME_DB_USER, "postgres"),
            "password": os.getenv(cls.VARNAME_DB_PASS, ""),
            "dbname": os.getenv(cls.VARNAME_DB_NAME, "migrateit"),
            "connect_timeout": int(os.getenv(cls.VARNAME_DB_TIMEOUT_SECONDS, C.DEFAULT_TIMEOUT_SECONDS)),
        }

    @override
    @classmethod
    def create_migrations_table_str(cls, table_name: str) -> tuple[str, str]:
        if not table_name.isidentifier():
            raise ValueError(f"Unsafe table name: {table_name}")
        migrations_query = f"""
CREATE TABLE IF NOT EXISTS {cls._q(table_name)} (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(255) UNIQUE NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_hash VARCHAR(64) NOT NULL,
    squashed BOOLEAN DEFAULT FALSE
);
"""
        reverse_query = f"""
DROP TABLE IF EXISTS {cls._q(table_name)};
"""
        return migrations_query, reverse_query

    @override
    def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)  # pyright: ignore

    @override
    def execute_for_one(self, query: str, params: tuple[Any, ...] = ()) -> Any:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)  # pyright: ignore
            return cursor.fetchone()

    @override
    def execute_for_rows(self, query: str, params: tuple[Any, ...] = ()) -> list[Any]:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)  # pyright: ignore
            return cursor.fetchall()

    @override
    def is_migrations_table_created(self) -> bool:
        query = """
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE LOWER(table_name) = LOWER(%s)
);
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query, (self.table_name,))
            result = cursor.fetchone()
            return result[0] if result else False

    @property
    @override
    def export_items(self) -> list[ExportItem]:
        return [
            ExportItem(
                name="schemas",
                metadata_query=(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name NOT IN (%s, %s, %s) AND schema_name NOT LIKE %s;"
                ),
                process_rows=partial(_process_schemas, self),
                query_params=("pg_catalog", "information_schema", "pg_toast", "pg_temp%"),
            ),
            ExportItem(
                name="enum types",
                metadata_query="""
SELECT n.nspname, t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder)
FROM pg_type t
    JOIN pg_enum e ON t.oid = e.enumtypid
    JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
GROUP BY n.nspname, t.typname;
                """,
                process_rows=partial(_process_enums, self),
            ),
            ExportItem(
                name="sequences",
                metadata_query="""
SELECT sequence_schema, sequence_name
FROM information_schema.sequences
WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema');
                """,
                process_rows=partial(_process_sequences, self),
            ),
            ExportItem(
                name="tables",
                metadata_query="""
SELECT c.table_schema, c.table_name, c.column_name, c.data_type, c.udt_name,
    c.character_maximum_length, c.is_nullable, c.column_default
FROM information_schema.columns c
    JOIN information_schema.tables t ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE t.table_type = 'BASE TABLE' AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
                """,
                process_rows=partial(_process_tables, self),
            ),
            ExportItem(
                name="functions and procedures",
                metadata_query="""
SELECT n.nspname, p.proname, pg_get_functiondef(p.oid), pg_get_function_identity_arguments(p.oid)
FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND p.prokind IN ('f', 'p');
                """,
                process_rows=partial(_process_functions, self),
            ),
            ExportItem(
                name="views",
                metadata_query="""
SELECT table_schema, table_name, view_definition
FROM information_schema.views
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
                """,
                process_rows=partial(_process_views, self),
            ),
            ExportItem(
                name="constraints",
                metadata_query="""
SELECT n.nspname, c.relname, con.conname, pg_get_constraintdef(con.oid)
FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
                """,
                process_rows=partial(_process_constraints, self),
            ),
            ExportItem(
                name="indexes",
                metadata_query="""
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
  AND indexname NOT IN (SELECT conname FROM pg_constraint WHERE contype IN ('p', 'u'));
                """,
                process_rows=partial(_process_indexes, self),
            ),
            ExportItem(
                name="triggers",
                metadata_query="""
SELECT n.nspname, c.relname, trig.tgname, pg_get_triggerdef(trig.oid)
FROM pg_trigger trig
    JOIN pg_class c ON c.oid = trig.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND NOT trig.tgisinternal;
                """,
                process_rows=partial(_process_triggers, self),
            ),
        ]

    @override
    def _patch_sql_statement(self, sql: str) -> str:
        sql = super()._patch_sql_statement(sql)
        if not any(w in sql for w in ("CREATE ", "ALTER ", "DROP ")):
            return sql
        if "ALTER TABLE" in sql:
            if "ADD COLUMN" in sql and "IF NOT EXISTS" not in sql:
                return sql.replace("ADD COLUMN", "ADD COLUMN IF NOT EXISTS", 1)
            if "DROP COLUMN" in sql and "IF EXISTS" not in sql:
                return sql.replace("DROP COLUMN", "DROP COLUMN IF EXISTS", 1)
        return sql


# ---------------------------------------------------------------------------
# Module-level row processors for each export item type
# ---------------------------------------------------------------------------


def _process_schemas(client: PsqlClient, rows: list[tuple[str]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for (schema,) in rows:
        if schema != "public":
            fwd.append(f"CREATE SCHEMA IF NOT EXISTS {client._q(schema)};")
            rb.append(f"DROP SCHEMA IF EXISTS {client._q(schema)} CASCADE;")
    return fwd, rb


def _process_enums(client: PsqlClient, rows: list[tuple[str, str, list[str]]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for schema, typname, labels in rows:
        formatted_labels = ", ".join(f"'{lbl}'" for lbl in labels)
        fwd.append(f"CREATE TYPE {client._q(schema)}.{client._q(typname)} AS ENUM ({formatted_labels});")
        rb.append(f"DROP TYPE IF EXISTS {client._q(schema)}.{client._q(typname)};")
    return fwd, rb


def _process_sequences(client: PsqlClient, rows: list[tuple[str, str]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for schema, seq_name in rows:
        fwd.append(f"CREATE SEQUENCE IF NOT EXISTS {client._q(schema)}.{client._q(seq_name)};")
        rb.append(f"DROP SEQUENCE IF EXISTS {client._q(schema)}.{client._q(seq_name)} CASCADE;")
    return fwd, rb


def _process_tables(client: PsqlClient, rows: list[tuple[Any, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    table_columns = defaultdict(list)
    for schema, table, col, dtype, udt_name, char_len, nullable, default in rows:
        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        if char_len and dtype in ("character varying", "character"):
            col_type = f"{dtype}({char_len})"
        elif dtype == "USER-DEFINED":
            col_type = f"{client._q(schema)}.{client._q(udt_name)}"
        else:
            col_type = dtype

        col_def = f"    {client._q(col)} {col_type}"
        if nullable == "NO":
            col_def += " NOT NULL"
        if default is not None:
            col_def += f" DEFAULT {default}"
        table_columns[(schema, table)].append(col_def)

    for (schema, table), cols in table_columns.items():
        cols_str = ",\n".join(cols)
        fwd.append(f"CREATE TABLE IF NOT EXISTS {client._q(schema)}.{client._q(table)} (\n{cols_str}\n);")
        rb.append(f"DROP TABLE IF EXISTS {client._q(schema)}.{client._q(table)} CASCADE;")
    return fwd, list(reversed(rb))


def _process_functions(client: PsqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for schema, name, func_def, args_sig in rows:
        if not re.match(r"(CREATE|ALTER)\s+(OR\s+REPLACE\s+)?(FUNCTION|PROCEDURE)", func_def, re.IGNORECASE):
            raise ValueError(f"Invalid function definition for {schema}.{name}")
        fwd.append(f"{func_def};")
        rb.append(f"DROP FUNCTION IF EXISTS {client._q(schema)}.{client._q(name)}({args_sig}) CASCADE;")
    return fwd, rb


def _process_views(client: PsqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for schema, view_name, view_def in rows:
        fwd.append(f"CREATE OR REPLACE VIEW {client._q(schema)}.{client._q(view_name)} AS\n{view_def.strip()};")
        rb.append(f"DROP VIEW IF EXISTS {client._q(schema)}.{client._q(view_name)};")
    return fwd, rb


def _process_constraints(client: PsqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd = []
    for schema, table, conname, condef in rows:
        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue
        fwd.append(
            f"ALTER TABLE ONLY {client._q(schema)}.{client._q(table)} ADD CONSTRAINT {client._q(conname)} {condef};"
        )
    return fwd, []


def _process_indexes(client: PsqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd = []
    for schema, table, indexname, indexdef in rows:
        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue
        fwd.append(f"{indexdef};")
    return fwd, []


def _process_triggers(client: PsqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd = []
    for schema, table, tgname, tgdef in rows:
        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue
        fwd.append(f"{tgdef};")
    return fwd, []
