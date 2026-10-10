import os
from collections import defaultdict
from functools import partial
from typing import TYPE_CHECKING, Any, override

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.clients._protocol import ExportItem

if TYPE_CHECKING:
    from mysql.connector.abstracts import MySQLConnectionAbstract  # noqa: F401
    from mysql.connector.pooling import PooledMySQLConnection  # noqa: F401


class MySqlClient(SqlClient["MySQLConnectionAbstract | PooledMySQLConnection"]):
    @override
    @classmethod
    def get_connection_params(cls) -> dict[str, Any]:
        db_url = os.getenv(cls.VARNAME_DB_URL)
        if db_url:
            return {"connection_string": db_url}

        return {
            "host": os.getenv(cls.VARNAME_DB_HOST, "localhost"),
            "port": int(os.getenv(cls.VARNAME_DB_PORT, "3306")),
            "user": os.getenv(cls.VARNAME_DB_USER, "root"),
            "password": os.getenv(cls.VARNAME_DB_PASS, ""),
            "database": os.getenv(cls.VARNAME_DB_NAME, "migrateit"),
            "connection_timeout": int(os.getenv(cls.VARNAME_DB_TIMEOUT_SECONDS, C.DEFAULT_TIMEOUT_SECONDS)),
        }

    @override
    @classmethod
    def create_migrations_table_str(cls, table_name: str) -> tuple[str, str]:
        if not table_name.isidentifier():
            raise ValueError(f"Unsafe table name: {table_name}")
        migrations_query = f"""
CREATE TABLE IF NOT EXISTS {cls._q(table_name)} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    migration_name VARCHAR(255) UNIQUE NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_hash VARCHAR(64) NOT NULL,
    squashed TINYINT(1) DEFAULT 0
);
        """
        reverse_query = f"""
DROP TABLE IF EXISTS {cls._q(table_name)};
        """
        return migrations_query, reverse_query

    @override
    @classmethod
    def _q(cls, name: str, QUOTE_CHAR: str = "`") -> str:
        return super()._q(name, QUOTE_CHAR)

    @override
    def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            if any(m in query.upper() for m in ("SELECT", "SHOW", "DESCRIBE")):
                cursor.fetchall()

    @override
    def execute_for_one(self, query: str, params: tuple[Any, ...] = ()) -> Any:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchone()

    @override
    def execute_for_rows(self, query: str, params: tuple[Any, ...] = ()) -> list[Any]:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    @override
    def is_migrations_table_created(self) -> bool:
        query = """
SELECT EXISTS(
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
        AND LOWER(table_name) = LOWER(%s)
) as value;
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query, (self.table_name,))
            result = cursor.fetchone()
            return bool(result[0]) if result else False  # type: ignore

    @property
    @override
    def export_items(self) -> list[ExportItem]:
        system_schemas = ("mysql", "information_schema", "performance_schema", "sys")
        schemas_filter = f"NOT IN ({', '.join('%s' for _ in system_schemas)})"
        return [
            ExportItem(
                name="schemas",
                metadata_query=(
                    f"SELECT schema_name FROM information_schema.schemata WHERE schema_name {schemas_filter};"
                ),
                process_rows=partial(_process_schemas, self),
                query_params=system_schemas,
            ),
            ExportItem(
                name="tables and columns",
                metadata_query=f"""
SELECT
    c.table_schema, c.table_name, c.column_name, c.column_type,
    c.is_nullable, c.column_default, c.extra
FROM information_schema.columns c
    JOIN information_schema.tables t ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE t.table_type = 'BASE TABLE' AND c.table_schema {schemas_filter}
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
                """,
                process_rows=partial(_process_tables, self),
                query_params=system_schemas,
            ),
            ExportItem(
                name="functions and procedures",
                metadata_query=f"""
SELECT routine_schema, routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema {schemas_filter};
                """,
                process_rows=partial(_process_routines, self),
                query_params=system_schemas,
            ),
            ExportItem(
                name="views",
                metadata_query=f"""
SELECT table_schema, table_name, view_definition
FROM information_schema.views
WHERE table_schema {schemas_filter};
                """,
                process_rows=partial(_process_views, self),
                query_params=system_schemas,
            ),
            ExportItem(
                name="constraints (PK, Unique)",
                metadata_query=f"""
SELECT
    tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type,
    GROUP_CONCAT(CONCAT('`', kcu.column_name, '`') ORDER BY kcu.ordinal_position SEPARATOR ', ') AS cols
FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_schema = kcu.constraint_schema
        AND tc.constraint_name = kcu.constraint_name
        AND tc.table_name = kcu.table_name
WHERE tc.constraint_schema {schemas_filter} AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
GROUP BY tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type;
                """,
                process_rows=partial(_process_pk_unique, self),
                query_params=system_schemas,
            ),
            ExportItem(
                name="foreign keys",
                metadata_query=f"""
SELECT
    tc.table_schema, tc.table_name, tc.constraint_name,
    GROUP_CONCAT(CONCAT('`', kcu.column_name, '`') ORDER BY kcu.ordinal_position SEPARATOR ', ') AS cols,
    kcu.referenced_table_schema, kcu.referenced_table_name,
    GROUP_CONCAT(CONCAT('`', kcu.referenced_column_name, '`') ORDER BY kcu.ordinal_position SEPARATOR ', ') AS ref_cols,
    rc.update_rule, rc.delete_rule
FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_schema = kcu.constraint_schema
        AND tc.constraint_name = kcu.constraint_name
        AND tc.table_name = kcu.table_name
    JOIN information_schema.referential_constraints rc
        ON tc.constraint_schema = rc.constraint_schema
        AND tc.constraint_name = rc.constraint_name
WHERE tc.constraint_schema {schemas_filter} AND tc.constraint_type = 'FOREIGN KEY'
GROUP BY tc.table_schema, tc.table_name, tc.constraint_name,
    kcu.referenced_table_schema, kcu.referenced_table_name, rc.update_rule, rc.delete_rule;
                """,
                process_rows=partial(_process_foreign_keys, self),
                query_params=system_schemas,
            ),
            ExportItem(
                name="check constraints",
                metadata_query=f"""
SELECT tc.table_schema, tc.table_name, tc.constraint_name, cc.check_clause
FROM information_schema.table_constraints tc
    JOIN information_schema.check_constraints cc
        ON tc.constraint_schema = cc.constraint_schema AND tc.constraint_name = cc.constraint_name
WHERE tc.constraint_schema {schemas_filter} AND tc.constraint_type = 'CHECK';
                """,
                process_rows=partial(_process_check_constraints, self),
                query_params=system_schemas,
            ),
            ExportItem(
                name="indexes",
                metadata_query=f"""
SELECT table_schema, table_name, index_name, non_unique,
    GROUP_CONCAT(CONCAT('`', column_name, '`') ORDER BY seq_in_index SEPARATOR ', ') AS cols
FROM information_schema.statistics
WHERE table_schema {schemas_filter} AND index_name != 'PRIMARY'
GROUP BY table_schema, table_name, index_name, non_unique;
                """,
                process_rows=partial(_process_indexes, self),
                query_params=system_schemas,
            ),
            ExportItem(
                name="triggers",
                metadata_query=f"""
SELECT trigger_schema, trigger_name, event_object_table
FROM information_schema.triggers
WHERE trigger_schema {schemas_filter};
                """,
                process_rows=partial(_process_triggers, self),
                query_params=system_schemas,
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
        return sql


# ---------------------------------------------------------------------------
# Module-level row processors
# ---------------------------------------------------------------------------


def _process_schemas(client: MySqlClient, rows: list[tuple[str]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for row in rows:
        schema = _to_str(row[0])
        fwd.append(f"CREATE DATABASE IF NOT EXISTS {client._q(schema)};")
        rb.append(f"DROP DATABASE IF EXISTS {client._q(schema)};")
    return fwd, rb


def _process_tables(client: MySqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    table_columns = defaultdict(list)
    for row in rows:
        schema = _to_str(row[0])
        table = _to_str(row[1])
        col = _to_str(row[2])
        col_type = _to_str(row[3])
        nullable = _to_str(row[4])
        default = row[5]
        extra = _to_str(row[6])

        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        col_def = f"    {client._q(col)} {col_type}"
        if nullable == "NO":
            col_def += " NOT NULL"
        if default is not None:
            default_str = _to_str(default)
            if default_str.upper() in ("CURRENT_TIMESTAMP", "NULL") or default_str.isdigit():
                col_def += f" DEFAULT {default_str}"
            else:
                col_def += f" DEFAULT '{default_str}'"
        if "auto_increment" in extra.lower():
            col_def += " AUTO_INCREMENT"
        table_columns[(schema, table)].append(col_def)

        for (schema, table), table_col_defs in table_columns.items():
            cols_str = ",\n".join(table_col_defs)
            fwd.append(f"CREATE TABLE IF NOT EXISTS {client._q(schema)}.{client._q(table)} (\n{cols_str}\n);")
            rb.append(f"DROP TABLE IF EXISTS {client._q(schema)}.{client._q(table)};")
    return fwd, list(reversed(rb))


def _process_routines(client: MySqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for row in rows:
        schema = _to_str(row[0])
        r_name = _to_str(row[1])
        r_type = _to_str(row[2])

        if r_type.upper() not in ("FUNCTION", "PROCEDURE"):
            raise ValueError(f"Unexpected routine type: {r_type!r}")

        show_row = client.execute_for_one(f"SHOW CREATE {r_type} {client._q(schema)}.{client._q(r_name)}")
        fwd.append(f"{_to_str(show_row[2])};")
        rb.append(f"DROP {r_type} IF EXISTS {client._q(schema)}.{client._q(r_name)};")
    return fwd, rb


def _process_views(client: MySqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for row in rows:
        schema = _to_str(row[0])
        view_name = _to_str(row[1])
        view_def = _to_str(row[2])

        fwd.append(f"CREATE OR REPLACE VIEW {client._q(schema)}.{client._q(view_name)} AS\n{view_def.strip()};")
        rb.append(f"DROP VIEW IF EXISTS {client._q(schema)}.{client._q(view_name)};")
    return fwd, rb


def _process_pk_unique(client: MySqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd = []
    for row in rows:
        schema = _to_str(row[0])
        table = _to_str(row[1])
        conname = _to_str(row[2])
        contype = _to_str(row[3])
        con_cols = _to_str(row[4])

        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        if contype == "PRIMARY KEY":
            fwd.append(f"ALTER TABLE {client._q(schema)}.{client._q(table)} ADD PRIMARY KEY ({con_cols});")
        elif contype == "UNIQUE":
            fwd.append(
                f"ALTER TABLE {client._q(schema)}.{client._q(table)} "
                f"ADD CONSTRAINT {client._q(conname)} UNIQUE ({con_cols});"
            )
        else:  # pragma: no cover[safety]
            raise ValueError(f"Unsupported constraint type: {contype}")
    return fwd, []


def _process_foreign_keys(client: MySqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd = []
    for row in rows:
        schema = _to_str(row[0])
        table = _to_str(row[1])
        conname = _to_str(row[2])
        fk_cols = _to_str(row[3])
        ref_schema = _to_str(row[4])
        ref_table = _to_str(row[5])
        ref_cols_str = _to_str(row[6])
        up_rule = _to_str(row[7])
        del_rule = _to_str(row[8])

        fwd.append(
            f"ALTER TABLE {client._q(schema)}.{client._q(table)} ADD CONSTRAINT {client._q(conname)} "
            f"FOREIGN KEY ({fk_cols}) REFERENCES {client._q(ref_schema)}.{client._q(ref_table)} ({ref_cols_str}) "
            f"ON UPDATE {up_rule} ON DELETE {del_rule};"
        )
    return fwd, []


def _process_check_constraints(client: MySqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd = []
    for row in rows:
        schema = _to_str(row[0])
        table = _to_str(row[1])
        conname = _to_str(row[2])
        check_clause = _to_str(row[3])

        fwd.append(
            f"ALTER TABLE {client._q(schema)}.{client._q(table)} "
            f"ADD CONSTRAINT {client._q(conname)} CHECK ({check_clause});"
        )
    return fwd, []


def _process_indexes(client: MySqlClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd = []
    for row in rows:
        schema = _to_str(row[0])
        table = _to_str(row[1])
        indexname = _to_str(row[2])
        non_unique = row[3]
        idx_cols = _to_str(row[4])

        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        unique_kw = "" if non_unique else "UNIQUE "
        fwd.append(
            f"CREATE {unique_kw}INDEX {client._q(indexname)} ON {client._q(schema)}.{client._q(table)} ({idx_cols});"
        )
    return fwd, []


def _process_triggers(client: MySqlClient, rows: list[tuple[str, str, str]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for row in rows:
        schema = _to_str(row[0])
        tgname = _to_str(row[1])
        table = _to_str(row[2])

        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        show_row = client.execute_for_one(f"SHOW CREATE TRIGGER {client._q(schema)}.{client._q(tgname)}")
        fwd.append(f"{_to_str(show_row[-1])};")
        rb.append(f"DROP TRIGGER IF EXISTS {client._q(schema)}.{client._q(tgname)};")
    return fwd, rb


def _to_str(val: Any) -> str:  # pragma: no cover[safety]
    """Safely convert database query values (bytes, Decimal, int, str, etc.) to str."""
    if val is None:
        return ""
    if isinstance(val, bytes):
        return val.decode("utf-8")
    return str(val)
