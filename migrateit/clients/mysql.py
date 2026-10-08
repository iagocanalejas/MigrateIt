import os
from collections import defaultdict
from typing import Any, TypedDict, cast, override

import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.models.migration import Migration, MigrationStatus, get_migration_header
from migrateit.reporters.logs import logger
from migrateit.reporters.output import write_line


def _to_str(val: Any) -> str:  # pragma: no cover
    """Safely convert database query values (bytes, Decimal, int, str, etc.) to str."""
    if val is None:
        return ""
    if isinstance(val, bytes):
        return val.decode("utf-8")
    return str(val)


def _extract_show_create(show_row: Any) -> str:  # pragma: no cover
    """Extract DDL text from SHOW CREATE result regardless of tuple or dict cursor format."""
    if not show_row:
        return ""
    if isinstance(show_row, dict):
        for k, v in show_row.items():
            k_str = _to_str(k).lower()
            if k_str.startswith("create") or k_str == "sql original statement":
                return _to_str(v)
        return _to_str(list(show_row.values())[-1])
    if isinstance(show_row, (tuple, list)):
        idx = 2 if len(show_row) > 2 else -1
        return _to_str(show_row[idx])
    return ""


class MySqlClient(SqlClient[MySQLConnectionAbstract | PooledMySQLConnection]):
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
    def is_migrations_table_created(self) -> bool:
        query = """
SELECT EXISTS(
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
        AND LOWER(table_name) = LOWER(%s)
) as value;
"""
        with self.connection.cursor(dictionary=True) as cursor:
            cursor.execute(query, (self.table_name,))
            result = cast(_BoolResult, cursor.fetchone())
            return result["value"]

    @override
    def is_migration_applied(self, migration: Migration) -> bool:
        query = f"""
SELECT EXISTS(
    SELECT 1 FROM {self._q(self.table_name)} WHERE migration_name = %s
) as value;
"""
        with self.connection.cursor(dictionary=True) as cursor:
            cursor.execute(query, (migration.name,))
            result = cast(_BoolResult, cursor.fetchone())
            return result["value"]

    @override
    def retrieve_migration_statuses(self) -> dict[str, MigrationStatus]:
        migrations = {k: MigrationStatus.NOT_APPLIED for k, _ in self.changelog.migrations_tree.items()}

        if not self.is_migrations_table_created():
            return migrations

        query = f"""
SELECT migration_name, change_hash
FROM {self._q(self.table_name)};
"""
        with self.connection.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        migrations_by_name = {m.name: m for m in self.changelog.migrations}
        for row in cast(list[_MigrationStatusRow], rows):
            migration_name, db_hash = row["migration_name"], row["change_hash"]
            migration = migrations_by_name.get(migration_name, None)
            if not migration:
                # migration applied not in changelog
                migrations[migration_name] = MigrationStatus.REMOVED
                continue

            _, _, migration_hash = self.get_migration_content_and_hash(self.migrations_dir / migration.name)
            status = MigrationStatus.APPLIED
            if migration_hash != db_hash:
                status = MigrationStatus.CONFLICT
                write_line(f"Hash mismatch for {migration_name}: file={migration_hash} db={db_hash}")
                logger.warning("Hash mismatch for %s: file=%s db=%s", migration_name, migration_hash, db_hash)

            migrations[migration.name] = status

        return migrations

    @override
    def apply_migration(self, migration: Migration, is_fake: bool = False, is_rollback: bool = False) -> None:
        path = self.get_migration_path(migration)
        if not migration.initial and not (self.is_migration_applied(migration) == is_rollback):
            if is_rollback:
                raise ValueError(f"Migration {path.name} is not applied, cannot undo it")
            raise ValueError(f"Migration {path.name} is already applied, cannot apply it again")

        migration_code, reverse_migration_code, migration_hash = self.get_migration_content_and_hash(path)

        try:
            with self.connection.cursor() as cursor:
                if not is_fake:
                    code = migration_code if not is_rollback else reverse_migration_code
                    cursor.execute(code)
                    if any(m in code.upper() for m in ("SELECT", "SHOW", "DESCRIBE")):
                        cursor.fetchall()
                self._update_migration_changelog(cursor, migration, migration_hash, is_rollback)
        except mysql.connector.Error as e:
            self.connection.rollback()
            raise e

    @override
    def squash_migrations(self, migrations: list[str], new_migration: Migration) -> None:
        placeholders = ", ".join(["%s"] * len(migrations))
        query = f"""
UPDATE {self._q(self.table_name)}
SET squashed=1
WHERE migration_name IN ({placeholders});
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query, migrations)
        self.apply_migration(new_migration, is_fake=True)

    @override
    def update_migration_hash(self, migration: Migration) -> None:
        path = self.get_migration_path(migration)
        _, _, migration_hash = self.get_migration_content_and_hash(path)

        query = f"""
UPDATE {self._q(self.table_name)}
SET change_hash = %s
WHERE migration_name = %s;
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query, (migration_hash, path.name))

    @override
    def export_database_schema(self, migration: Migration) -> None:
        if len(migration.parents) != 1 or migration.parents[0] != self.changelog.root.name:
            raise ValueError("Full database export must depend only on the initial migration")

        forward_ddl = []
        rollback_ddl = []
        system_schemas = ("mysql", "information_schema", "performance_schema", "sys")

        write_line(f"Exporting full database schema to '{migration.name}'...")
        with self.connection.cursor(dictionary=True) as cursor:
            # -------------------------------------------------------------
            # 1. SCHEMAS / DATABASES
            # -------------------------------------------------------------
            write_line("\tExporting schemas...")
            cursor.execute(
                """
SELECT schema_name as schema_name
FROM information_schema.schemata
WHERE schema_name NOT IN (%s, %s, %s, %s);
            """,
                system_schemas,
            )
            for r1 in cast(list[_SchemaRow], cursor.fetchall()):
                schema = _to_str(r1["schema_name"])
                forward_ddl.append(f"CREATE DATABASE IF NOT EXISTS {self._q(schema)};")
                rollback_ddl.append(f"DROP DATABASE IF EXISTS {self._q(schema)};")

            # -------------------------------------------------------------
            # 2. TABLES & COLUMNS
            # -------------------------------------------------------------
            write_line("\tExporting tables and columns...")
            cursor.execute(
                """
SELECT
    c.table_schema as table_schema,
    c.table_name as table_name,
    c.column_name as column_name,
    c.column_type as column_type,
    c.is_nullable as is_nullable,
    c.column_default as column_default,
    c.extra as extra
FROM information_schema.columns c
    JOIN information_schema.tables t ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE t.table_type = 'BASE TABLE'
    AND c.table_schema NOT IN (%s, %s, %s, %s)
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
            """,
                system_schemas,
            )

            table_columns = defaultdict(list)
            for r2 in cast(list[_ColumnRow], cursor.fetchall()):
                schema = _to_str(r2["table_schema"])
                table = _to_str(r2["table_name"])
                col = _to_str(r2["column_name"])
                col_type = _to_str(r2["column_type"])
                nullable = _to_str(r2["is_nullable"])
                default = r2["column_default"]
                extra = _to_str(r2["extra"])

                col_def = f"    {self._q(col)} {col_type}"
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

            tables_list = []
            for (schema, table), table_col_defs in table_columns.items():
                if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                    continue
                cols_str = ",\n".join(table_col_defs)
                forward_ddl.append(f"CREATE TABLE IF NOT EXISTS {self._q(schema)}.{self._q(table)} (\n{cols_str}\n);")
                tables_list.append((schema, table))

            for schema, table in reversed(tables_list):
                rollback_ddl.append(f"DROP TABLE IF EXISTS {self._q(schema)}.{self._q(table)};")

            # -------------------------------------------------------------
            # 3. FUNCTIONS & PROCEDURES
            # -------------------------------------------------------------
            write_line("\tExporting functions and procedures...")
            cursor.execute(
                """
SELECT
    routine_schema as routine_schema,
    routine_name as routine_name,
    routine_type as routine_type
FROM information_schema.routines
WHERE routine_schema NOT IN (%s, %s, %s, %s);
            """,
                system_schemas,
            )

            for r3 in cast(list[_RoutineRow], cursor.fetchall()):
                schema = _to_str(r3["routine_schema"])
                r_name = _to_str(r3["routine_name"])
                r_type = _to_str(r3["routine_type"])

                if r_type.upper() not in ("FUNCTION", "PROCEDURE"):
                    raise ValueError(f"Unexpected routine type: {r_type!r}")

                cursor.execute(f"SHOW CREATE {r_type} {self._q(schema)}.{self._q(r_name)}")
                show_row = cursor.fetchone()
                func_def = _extract_show_create(show_row)
                forward_ddl.append(f"{func_def};")
                rollback_ddl.append(f"DROP {r_type} IF EXISTS {self._q(schema)}.{self._q(r_name)};")

            # -------------------------------------------------------------
            # 4. VIEWS
            # -------------------------------------------------------------
            write_line("\tExporting views...")
            cursor.execute(
                """
SELECT
    table_schema as table_schema,
    table_name as table_name,
    view_definition as view_definition
FROM information_schema.views
WHERE table_schema NOT IN (%s, %s, %s, %s);
            """,
                system_schemas,
            )
            for r4 in cast(list[_ViewRow], cursor.fetchall()):
                schema = _to_str(r4["table_schema"])
                view_name = _to_str(r4["table_name"])
                view_def = _to_str(r4["view_definition"])

                forward_ddl.append(
                    f"CREATE OR REPLACE VIEW {self._q(schema)}.{self._q(view_name)} AS\n{view_def.strip()};"
                )
                rollback_ddl.append(f"DROP VIEW IF EXISTS {self._q(schema)}.{self._q(view_name)};")

            # -------------------------------------------------------------
            # 5. CONSTRAINTS (Primary Keys, Foreign Keys, Unique)
            # -------------------------------------------------------------
            write_line("\tExporting constraints...")
            # Primary Keys and Unique Constraints
            cursor.execute(
                """
SELECT
    tc.table_schema as table_schema,
    tc.table_name as table_name,
    tc.constraint_name as constraint_name,
    tc.constraint_type as constraint_type,
    GROUP_CONCAT(CONCAT('`', kcu.column_name, '`') ORDER BY kcu.ordinal_position SEPARATOR ', ') AS cols
FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_schema = kcu.constraint_schema
        AND tc.constraint_name = kcu.constraint_name
        AND tc.table_name = kcu.table_name
WHERE tc.constraint_schema NOT IN (%s, %s, %s, %s) AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
GROUP BY tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type;
            """,
                system_schemas,
            )

            for r5 in cast(list[_ConstraintRow], cursor.fetchall()):
                schema = _to_str(r5["table_schema"])
                table = _to_str(r5["table_name"])
                conname = _to_str(r5["constraint_name"])
                contype = _to_str(r5["constraint_type"])
                con_cols = _to_str(r5["cols"])

                if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                    continue
                if contype == "PRIMARY KEY":
                    forward_ddl.append(f"ALTER TABLE {self._q(schema)}.{self._q(table)} ADD PRIMARY KEY ({con_cols});")
                elif contype == "UNIQUE":
                    forward_ddl.append(
                        f"ALTER TABLE {self._q(schema)}.{self._q(table)} "
                        f"ADD CONSTRAINT {self._q(conname)} UNIQUE ({con_cols});"
                    )
                else:  # pragma: no cover[safety]
                    raise ValueError(f"Unsupported constraint type: {contype}")

            # Foreign Keys
            cursor.execute(
                """
SELECT
    tc.table_schema as table_schema,
    tc.table_name as table_name,
    tc.constraint_name as constraint_name,
    GROUP_CONCAT(CONCAT('`', kcu.column_name, '`') ORDER BY kcu.ordinal_position SEPARATOR ', ') AS cols,
    kcu.referenced_table_schema as ref_table_schema,
    kcu.referenced_table_name as ref_table_name,
    GROUP_CONCAT(CONCAT('`', kcu.referenced_column_name, '`') ORDER BY kcu.ordinal_position SEPARATOR ', ') AS ref_cols,
    rc.update_rule as update_rule,
    rc.delete_rule as delete_rule
FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_schema = kcu.constraint_schema
        AND tc.constraint_name = kcu.constraint_name
        AND tc.table_name = kcu.table_name
    JOIN information_schema.referential_constraints rc
        ON tc.constraint_schema = rc.constraint_schema
        AND tc.constraint_name = rc.constraint_name
WHERE tc.constraint_schema NOT IN (%s, %s, %s, %s) AND tc.constraint_type = 'FOREIGN KEY'
GROUP BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.referenced_table_schema, kcu.referenced_table_name,
    rc.update_rule, rc.delete_rule;
""",
                system_schemas,
            )

            for r6 in cast(list[_ForeignKeyRow], cursor.fetchall()):
                schema = _to_str(r6["table_schema"])
                table = _to_str(r6["table_name"])
                conname = _to_str(r6["constraint_name"])
                fk_cols = _to_str(r6["cols"])
                ref_schema = _to_str(r6["ref_table_schema"])
                ref_table = _to_str(r6["ref_table_name"])
                ref_cols_str = _to_str(r6["ref_cols"])
                up_rule = _to_str(r6["update_rule"])
                del_rule = _to_str(r6["delete_rule"])

                fk_def = (
                    f"ALTER TABLE {self._q(schema)}.{self._q(table)} ADD CONSTRAINT {self._q(conname)} "
                    f"FOREIGN KEY ({fk_cols}) REFERENCES {self._q(ref_schema)}.{self._q(ref_table)} ({ref_cols_str}) "
                    f"ON UPDATE {up_rule} ON DELETE {del_rule};"
                )
                forward_ddl.append(fk_def)

            cursor.execute(
                """
SELECT
    tc.table_schema as table_schema,
    tc.table_name as table_name,
    tc.constraint_name as constraint_name,
    cc.check_clause as check_clause
FROM information_schema.table_constraints tc
    JOIN information_schema.check_constraints cc
        ON tc.constraint_schema = cc.constraint_schema AND tc.constraint_name = cc.constraint_name
WHERE tc.constraint_schema NOT IN (%s, %s, %s, %s)
    AND tc.constraint_type = 'CHECK';
                """,
                system_schemas,
            )

            for r7 in cast(list[_CheckConstraintRow], cursor.fetchall()):
                schema = _to_str(r7["table_schema"])
                table = _to_str(r7["table_name"])
                conname = _to_str(r7["constraint_name"])
                check_clause = _to_str(r7["check_clause"])

                forward_ddl.append(
                    f"ALTER TABLE {self._q(schema)}.{self._q(table)} "
                    f"ADD CONSTRAINT {self._q(conname)} CHECK ({check_clause});"
                )

            # -------------------------------------------------------------
            # 6. INDEXES
            # -------------------------------------------------------------
            write_line("\tExporting indexes...")
            cursor.execute(
                """
SELECT
    table_schema as table_schema,
    table_name as table_name,
    index_name as index_name,
    non_unique as non_unique,
    GROUP_CONCAT(CONCAT('`', column_name, '`') ORDER BY seq_in_index SEPARATOR ', ') AS cols
FROM information_schema.statistics
WHERE table_schema NOT IN (%s, %s, %s, %s) AND index_name != 'PRIMARY'
GROUP BY table_schema, table_name, index_name, non_unique;
            """,
                system_schemas,
            )

            for r8 in cast(list[_IndexRow], cursor.fetchall()):
                schema = _to_str(r8["table_schema"])
                table = _to_str(r8["table_name"])
                indexname = _to_str(r8["index_name"])
                non_unique = r8["non_unique"]
                idx_cols = _to_str(r8["cols"])

                if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                    continue
                unique_kw = "" if non_unique else "UNIQUE "
                forward_ddl.append(
                    f"CREATE {unique_kw}INDEX {self._q(indexname)} ON {self._q(schema)}.{self._q(table)} ({idx_cols});"
                )

            # -------------------------------------------------------------
            # 7. TRIGGERS
            # -------------------------------------------------------------
            write_line("\tExporting triggers...")
            cursor.execute(
                """
SELECT
    trigger_schema as trigger_schema,
    trigger_name as trigger_name,
    event_object_table as event_object_table
FROM information_schema.triggers
WHERE trigger_schema NOT IN (%s, %s, %s, %s);
            """,
                system_schemas,
            )

            for r9 in cast(list[_TriggerRow], cursor.fetchall()):
                schema = _to_str(r9["trigger_schema"])
                tgname = _to_str(r9["trigger_name"])
                table = _to_str(r9["event_object_table"])
                if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                    continue

                cursor.execute(f"SHOW CREATE TRIGGER {self._q(schema)}.{self._q(tgname)}")
                show_row = cursor.fetchone()
                forward_ddl.append(f"{_extract_show_create(show_row)};")
                rollback_ddl.append(f"DROP TRIGGER IF EXISTS {self._q(schema)}.{self._q(tgname)};")

        migration_path = self.migrations_dir / migration.name
        with open(migration_path, "w", encoding="utf-8") as f:
            f.write(get_migration_header(migration_path))
            f.write("-- Migration automatically generated by migrateit\n\n")
            f.write("\n\n".join(forward_ddl) + "\n\n\n")
            f.write(C.ROLLBACK_SPLIT_TAG + "\n\n\n")
            f.write("\n\n".join(reversed(rollback_ddl)) + "\n")

    @override
    def _patch_sql_statement(self, sql: str) -> str:
        sql = super()._patch_sql_statement(sql)
        if not any(w in sql for w in ("CREATE ", "ALTER ", "DROP ")):
            return sql
        if "ALTER TABLE" in sql:
            if "ADD COLUMN" in sql and "IF NOT EXISTS" not in sql:
                return sql.replace("ADD COLUMN", "ADD COLUMN IF NOT EXISTS", 1)
        return sql

    @override
    def _get_database_hash(self, migration_name: str) -> str:
        query = f"""
SELECT change_hash
FROM {self._q(self.table_name)}
WHERE migration_name = %s;
"""
        with self.connection.cursor(dictionary=True) as cursor:
            cursor.execute(query, (migration_name,))
            result = cast(_HashResult, cursor.fetchone())

            if result and result["change_hash"]:
                return result["change_hash"]
            raise ValueError(f"Migration {migration_name} not found in the database")

    def _update_migration_changelog(
        self,
        cursor: Any,
        migration: Migration,
        hash: str,
        is_rollback: bool,
    ) -> None:
        if migration.initial and is_rollback:
            return

        path = self.migrations_dir / migration.name
        if is_rollback and not migration.initial:
            query = f"""
DELETE FROM {self._q(self.table_name)}
WHERE migration_name = %s
    AND change_hash = %s;
"""
        else:
            query = f"""
INSERT INTO {self._q(self.table_name)} (migration_name, change_hash)
VALUES (%s, %s);
"""
        cursor.execute(query, (path.name, hash))


class _BoolResult(TypedDict):
    value: bool


class _MigrationStatusRow(TypedDict):
    migration_name: str
    change_hash: str


class _HashResult(TypedDict):
    change_hash: str


class _SchemaRow(TypedDict):
    schema_name: str


class _ColumnRow(TypedDict):
    table_schema: str
    table_name: str
    column_name: str
    column_type: str
    is_nullable: str
    column_default: Any
    extra: str


class _RoutineRow(TypedDict):
    routine_schema: str
    routine_name: str
    routine_type: str


class _ViewRow(TypedDict):
    table_schema: str
    table_name: str
    view_definition: str


class _ConstraintRow(TypedDict):
    table_schema: str
    table_name: str
    constraint_name: str
    constraint_type: str
    cols: str


class _ForeignKeyRow(TypedDict):
    table_schema: str
    table_name: str
    constraint_name: str
    cols: str
    ref_table_schema: str
    ref_table_name: str
    ref_cols: str
    update_rule: str
    delete_rule: str


class _CheckConstraintRow(TypedDict):
    table_schema: str
    table_name: str
    constraint_name: str
    check_clause: str


class _IndexRow(TypedDict):
    table_schema: str
    table_name: str
    index_name: str
    non_unique: Any
    cols: str


class _TriggerRow(TypedDict):
    trigger_schema: str
    trigger_name: str
    event_object_table: str
