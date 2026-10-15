from __future__ import annotations

import os
from collections import defaultdict
from functools import partial
from typing import TYPE_CHECKING, Any, override

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.clients._protocol import ExportItem

if TYPE_CHECKING:
    import oracledb  # noqa: F401


class OracleClient(SqlClient["oracledb.Connection"]):
    VARNAME_DB_SID = os.getenv("VARNAME_DB_SID", "DB_SID")

    @property
    @override
    def placeholder(self) -> str:
        return ":param"

    @override
    @classmethod
    def get_connection_params(cls) -> dict[str, Any]:
        db_url = os.getenv(cls.VARNAME_DB_URL)
        if db_url:
            return {"dsn": db_url}

        return {
            "host": os.getenv(cls.VARNAME_DB_HOST, "localhost"),
            "port": int(os.getenv(cls.VARNAME_DB_PORT, "1521")),
            "user": os.getenv(cls.VARNAME_DB_USER, "oracle"),
            "password": os.getenv(cls.VARNAME_DB_PASS, ""),
            "service_name": os.getenv(cls.VARNAME_DB_NAME, "migrateit"),
            "sid": os.getenv(cls.VARNAME_DB_SID),
        }

    @override
    @classmethod
    def create_migrations_table_str(cls, table_name: str) -> tuple[str, str]:
        if not table_name.isidentifier():
            raise ValueError(f"Unsafe table name: {table_name}")
        seq_name = f"{table_name}_seq"
        migrations_query = f"""
CREATE SEQUENCE {seq_name} START WITH 1 INCREMENT BY 1 NOCACHE;

CREATE TABLE {cls._q(table_name)} (
    id NUMBER DEFAULT {seq_name}.NEXTVAL PRIMARY KEY,
    migration_name VARCHAR2(255) UNIQUE NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_hash VARCHAR2(64) NOT NULL,
    squashed NUMBER(1) DEFAULT 0
);
"""
        reverse_query = f"""
DROP TABLE {cls._q(table_name)} CASCADE CONSTRAINTS;
DROP SEQUENCE {seq_name};
"""
        return migrations_query, reverse_query

    @override
    def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        formatted_query, bound_params = self._format_params(query, params)
        with self.connection.cursor() as cursor:
            cursor.execute(formatted_query, bound_params)

    @override
    def execute_for_one(self, query: str, params: tuple[Any, ...] = ()) -> Any:
        formatted_query, bound_params = self._format_params(query, params)
        with self.connection.cursor() as cursor:
            cursor.execute(formatted_query, bound_params)
            return cursor.fetchone()

    @override
    def execute_for_rows(self, query: str, params: tuple[Any, ...] = ()) -> list[Any]:
        formatted_query, bound_params = self._format_params(query, params)
        with self.connection.cursor() as cursor:
            cursor.execute(formatted_query, bound_params)
            return cursor.fetchall()

    @override
    def is_migrations_table_created(self) -> bool:
        query = """
SELECT COUNT(*)
FROM user_tables
WHERE UPPER(table_name) = UPPER(:1)
"""
        result = self.execute_for_one(query, (self.table_name,))
        return result is not None and result[0] == 1

    @property
    @override
    def export_items(self) -> list[ExportItem]:
        return [
            ExportItem(
                name="tables and columns",
                metadata_query="""
SELECT Owner, Table_Name, Column_Name, Data_Type, Data_Length, Data_Precision, Data_Scale, Nullable, Data_Default
FROM all_tab_columns
WHERE Owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
ORDER BY Table_Name, Column_ID;
                """,
                process_rows=partial(_process_tables, self),
            ),
            ExportItem(
                name="indexes",
                metadata_query="""
SELECT i.Index_Name, i.Table_Name, i.Uniqueness, i.Index_Type,
        LISTAGG(ic.Column_Name, ', ') WITHIN GROUP (ORDER BY ic.Column_Position) AS Column_List,
        CASE WHEN i.Index_Type LIKE '%REV%' THEN 'YES' ELSE 'NO' END AS Reverse
FROM all_indexes i
    JOIN all_ind_columns ic
        ON i.Index_Name = ic.Index_Name
        AND i.Table_Name = ic.Table_Name
        AND i.Owner = ic.Table_Owner
WHERE i.Owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
    AND i.Table_Name NOT IN (
        SELECT Table_Name FROM all_tab_columns
        WHERE Column_Name = 'MIGRATION_NAME' AND Owner = i.Owner
    )
GROUP BY i.Index_Name, i.Table_Name, i.Uniqueness, i.Index_Type
ORDER BY i.Index_Name;
                """,
                process_rows=partial(_process_indexes, self),
            ),
            ExportItem(
                name="constraints (PK, Unique)",
                metadata_query="""
SELECT tc.Constraint_Name, tc.Constraint_Type, tc.Table_Name,
        LISTAGG(rc.Column_Name, ', ') WITHIN GROUP (ORDER BY rc.Position) AS Columns
FROM all_constraints tc
    JOIN all_cons_columns rc
        ON tc.Constraint_Name = rc.Constraint_Name
        AND tc.Owner = rc.Owner
WHERE tc.Owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
    AND tc.Table_Name NOT IN (
        SELECT Table_Name FROM all_tab_columns
        WHERE Column_Name = 'MIGRATION_NAME' AND Owner = tc.Owner
    )
GROUP BY tc.Constraint_Name, tc.Constraint_Type, tc.Table_Name
ORDER BY tc.Table_Name, tc.Constraint_Name;
                """,
                process_rows=partial(_process_constraints, self),
            ),
            ExportItem(
                name="foreign keys",
                metadata_query="""
SELECT tc.Constraint_Name, tc.Table_Name, tc.Status,
        LISTAGG(rc.Column_Name, ', ') WITHIN GROUP (ORDER BY rc.Position) AS Columns,
        rr.Ref_Owner, rr.Ref_Table, rr.Ref_Columns
FROM all_constraints tc
    JOIN all_cons_columns rc ON tc.Constraint_Name = rc.Constraint_Name AND tc.Owner = rc.Owner
    JOIN (
        SELECT c.Constraint_Name, c.Owner, c.Table_Name, cc.Column_Name, cc.Position,
            r.Owner AS Ref_Owner, r.Table_Name AS Ref_Table,
            LISTAGG(rl.Column_Name, ', ') WITHIN GROUP (ORDER BY rl.Position) AS Ref_Columns
        FROM all_constraints c
            JOIN all_cons_columns cc ON c.Constraint_Name = cc.Constraint_Name AND c.Owner = cc.Owner
            JOIN all_constraints r ON c.R_Constraint_Name = r.Constraint_Name AND c.R_Owner = r.Owner
            JOIN all_cons_columns rl ON r.Constraint_Name = rl.Constraint_Name AND r.Owner = rl.Owner
                AND cc.Position = rl.Position
        WHERE c.Constraint_Type = 'R'
                AND c.Owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
        GROUP BY c.Constraint_Name, c.Owner, c.Table_Name, cc.Column_Name, cc.Position,
            r.Owner, r.Table_Name
    ) rr ON tc.Constraint_Name = rr.Constraint_Name AND tc.Owner = rr.Owner
WHERE tc.Constraint_Type = 'R'
    AND tc.Owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
GROUP BY tc.Constraint_Name, tc.Table_Name, tc.Status,
    rr.Ref_Owner, rr.Ref_Table, rr.Ref_Columns
ORDER BY tc.Table_Name, tc.Constraint_Name;
                """,
                process_rows=partial(_process_foreign_keys, self),
            ),
            ExportItem(
                name="views",
                metadata_query="""
SELECT View_Name, Text_Length
FROM all_views
WHERE Owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
ORDER BY View_Name;
                """,
                process_rows=partial(_process_views, self),
            ),
            ExportItem(
                name="sequences",
                metadata_query="""
SELECT Sequence_Name, Min_Value, Max_Value, Increment_By, Cache_Size, Cycle_Flag
FROM user_sequences
WHERE Sequence_Name NOT IN (
    SELECT sequence_name
    FROM user_tab_identity_cols
)
ORDER BY Sequence_Name;
                """,
                process_rows=partial(_process_sequences, self),
            ),
            ExportItem(
                name="triggers",
                metadata_query=f"""
SELECT Trigger_Name, Table_Name, Status
FROM user_triggers
WHERE Table_Name != '{self.table_name.upper()}'
ORDER BY Trigger_Name;
            """,
                process_rows=partial(_process_triggers, self),
            ),
        ]

    @staticmethod
    def _format_params(query: str, params: tuple[Any, ...]) -> tuple[str, dict[str, Any]]:
        query = query.strip()
        if query.endswith(";"):
            query = query[:-1]
        if not params:
            return query, {}
        formatted = query
        for i, _ in enumerate(params, start=1):
            formatted = formatted.replace(":param", f":{i}", 1)
        return formatted, {str(i): v for i, v in enumerate(params, start=1)}


# ---------------------------------------------------------------------------
# Module-level row processors for each export item type
# ---------------------------------------------------------------------------


def _process_tables(client: OracleClient, rows: list[tuple[Any, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    table_columns: dict[str, list[str]] = defaultdict(list)

    for row in rows:
        table = _to_str(row[1])
        col = _to_str(row[2])
        col_type = _to_str(row[3])
        char_len = row[4]
        precision = row[5]
        scale = row[6]
        nullable = _to_str(row[7])
        default = row[8]

        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        # Build column type
        if char_len and col_type in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"):
            col_type = f"{col_type}({char_len})"
        elif precision is not None and col_type in ("NUMBER", "FLOAT"):
            if scale:
                col_type = f"{col_type}({precision},{scale})"
            else:
                col_type = f"{col_type}({precision})"

        col_def = f"    {client._q(col)} {col_type}"
        if nullable == "N":
            col_def += " NOT NULL"
        if default is not None:
            # Oracle stores defaults differently - strip leading whitespace and handle
            default_str = str(default).strip()
            if default_str and default_str.upper() != "NULL":
                col_def += f" DEFAULT {default_str}"

        table_columns[table].append(col_def)

    for table, cols in table_columns.items():
        cols_str = ",\n".join(cols)
        fwd.append(f"CREATE TABLE {client._q(table)} (\n{cols_str}\n);")
        rb.append(f"DROP TABLE {client._q(table)};")
    return fwd, list(reversed(rb))


def _process_indexes(client: OracleClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for index_name, table, uniqueness, index_type, column_list, inverse in rows:
        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        unique_kw = "UNIQUE " if uniqueness == "UNIQUE" else ""
        type_kw = " BITMAP " if index_type == "BITMAP" else " "
        fwd.append(f"CREATE {unique_kw}{type_kw}INDEX {client._q(index_name)} ON {client._q(table)} ({column_list});")
        rb.append(f"DROP INDEX {client._q(index_name)};")
    return fwd, rb


def _process_constraints(client: OracleClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd: list[str] = []
    rb: list[str] = []
    for conname, contype, table, cols in rows:
        if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        conname_str = _to_str(conname)
        contype_str = _to_str(contype)
        table_str = _to_str(table)
        cols_str = _to_str(cols)

        if contype_str == "P":
            fwd.append(
                f"ALTER TABLE {client._q(table_str)} ADD CONSTRAINT {client._q(conname_str)} PRIMARY KEY ({cols_str});"
            )
        elif contype_str == "U":
            fwd.append(
                f"ALTER TABLE {client._q(table_str)} ADD CONSTRAINT {client._q(conname_str)} UNIQUE ({cols_str});"
            )
        elif contype_str == "C":
            show_row = client.execute_for_one(
                "SELECT DBMS_METADATA.GET_DDL('CONSTRAINT', :param) FROM DUAL",
                (conname_str,),
            )
            con_ddl = str(show_row[0]).strip()
            fwd.append(con_ddl)
    return fwd, rb


def _process_foreign_keys(client: OracleClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for conname, table, status, cols, ref_owner, ref_table, ref_cols in rows:
        conname_str = _to_str(conname)
        table_str = _to_str(table)
        ref_table_str = _to_str(ref_table)
        cols_str = _to_str(cols)
        ref_cols_str = _to_str(ref_cols)

        fwd.append(
            f"ALTER TABLE {client._q(table_str)} ADD CONSTRAINT {client._q(conname_str)} "
            f"FOREIGN KEY ({cols_str}) REFERENCES {client._q(ref_table_str)} ({ref_cols_str});"
        )
        rb.append(f"ALTER TABLE {client._q(table_str)} DROP CONSTRAINT {client._q(conname_str)};")
    return fwd, rb


def _process_views(client: OracleClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for view_name, text_length in rows:
        view_name_str = _to_str(view_name)
        text_length_int = int(text_length) if text_length else 0

        if text_length_int > 4000:
            # Use CLOB view definition
            query = (
                "SELECT Text FROM all_views "
                "WHERE Owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') "
                "AND View_Name = :param"
            )
            view_def_rows = client.execute_for_rows(query, (view_name_str,))
            view_def = _to_str(view_def_rows[0][0]) if view_def_rows else ""
        else:
            view_def_rows = client.execute_for_one(
                (
                    "SELECT Text FROM all_views "
                    "WHERE Owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') "
                    "AND View_Name = :param"
                ),
                (view_name_str,),
            )
            view_def = _to_str(view_def_rows[0]) if view_def_rows else ""

        fwd.append(f"CREATE OR REPLACE VIEW {client._q(view_name_str)} AS\n{view_def.strip()};")
        rb.append(f"DROP VIEW {client._q(view_name_str)};")
    return fwd, rb


def _process_sequences(client: OracleClient, rows: list[tuple[str, ...]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for seq_name, min_val, max_val, increment, cache, cycle in rows:
        seq_name_str = _to_str(seq_name)
        min_val_str = _to_str(min_val) if min_val else "1"
        max_val_str = _to_str(max_val) if max_val else "999999999999999999999999999"
        increment_str = _to_str(increment) if increment else "1"
        cache_str = _to_str(cache) if cache else "20"
        cycle_str = "CYCLE" if _to_str(cycle) == "Y" else "NO CYCLE"

        fwd.append(
            f"CREATE SEQUENCE {client._q(seq_name_str)} "
            f"START WITH {min_val_str} MAXVALUE {max_val_str} "
            f"INCREMENT BY {increment_str} {cache_str} {cycle_str};"
        )
        rb.append(f"DROP SEQUENCE {client._q(seq_name_str)};")
    return fwd, rb


def _process_triggers(client: OracleClient, rows: list[tuple[str, str, str]]) -> tuple[list[str], list[str]]:
    fwd, rb = [], []
    for trigger_name, table, status in rows:
        trigger_name_str = _to_str(trigger_name)
        table_str = _to_str(table)

        if table_str.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
            continue

        show_row = client.execute_for_one(
            "SELECT DBMS_METADATA.GET_DDL('TRIGGER', :param) FROM DUAL",
            (trigger_name_str,),
        )

        trigger_ddl = str(show_row[0]).strip()
        fwd.append(trigger_ddl)
        rb.append(f"DROP TRIGGER {client._q(trigger_name_str)};")
    return fwd, rb


def _to_str(val: Any) -> str:  # pragma: no cover[safety]
    """Safely convert database query values to str."""
    if val is None:
        return ""
    if isinstance(val, bytes):
        return val.decode("utf-8")
    return str(val)
