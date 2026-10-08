import os
from collections import defaultdict
from typing import Any, override

import psycopg

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.models.migration import Migration, MigrationStatus, get_migration_header
from migrateit.reporters.logs import logger
from migrateit.reporters.output import write_line


class PsqlClient(SqlClient[psycopg.Connection]):
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

    @override
    def is_migration_applied(self, migration: Migration) -> bool:
        query = f"""
SELECT EXISTS (
    SELECT 1 FROM {self._q(self.table_name)} WHERE migration_name = %s
);
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query, (migration.name,))  # pyright: ignore
            result = cursor.fetchone()
            return result[0] if result else False

    @override
    def retrieve_migration_statuses(self) -> dict[str, MigrationStatus]:
        migrations = {k: MigrationStatus.NOT_APPLIED for k, _ in self.changelog.migrations_tree.items()}

        if not self.is_migrations_table_created():
            return migrations

        query = f"""
SELECT migration_name, change_hash
FROM {self._q(self.table_name)};
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)  # pyright: ignore
            rows = cursor.fetchall()

        migrations_by_name = {m.name: m for m in self.changelog.migrations}
        for row in rows:
            migration_name, db_hash = row
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
                    cursor.execute(code)  # pyright: ignore
                self._update_migration_changelog(cursor, migration, migration_hash, is_rollback)
        except (psycopg.DatabaseError, psycopg.ProgrammingError) as e:  # pragma: no cover
            self.connection.rollback()
            raise e

    @override
    def squash_migrations(self, migrations: list[str], new_migration: Migration) -> None:
        query = f"""
UPDATE {self._q(self.table_name)}
SET squashed = TRUE
WHERE migration_name = ANY(%s);
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query, (migrations,))  # pyright: ignore
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
            cursor.execute(query, (path.name, migration_hash))  # pyright: ignore

    @override
    def export_database_schema(self, migration: Migration) -> None:
        if len(migration.parents) != 1 or migration.parents[0] != self.changelog.root.name:
            raise ValueError("Full database export must depend only on the initial migration")

        forward_ddl = []
        rollback_ddl = []
        write_line(f"Exporting full database schema to '{migration.name}'...")
        with self.connection.cursor() as cursor:
            # -------------------------------------------------------------
            # 1. SCHEMAS
            # -------------------------------------------------------------
            write_line("\tExporting schemas...")
            cursor.execute("""
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND schema_name NOT LIKE 'pg_temp%';
            """)
            for (schema,) in cursor.fetchall():
                if schema != "public":
                    forward_ddl.append(f"CREATE SCHEMA IF NOT EXISTS {self._q(schema)};")
                    rollback_ddl.append(f"DROP SCHEMA IF EXISTS {self._q(schema)} CASCADE;")

            # -------------------------------------------------------------
            # 2. ENUM TYPES
            # -------------------------------------------------------------
            write_line("\tExporting enum types...")
            cursor.execute("""
SELECT n.nspname, t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder)
FROM pg_type t
    JOIN pg_enum e ON t.oid = e.enumtypid
    JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
GROUP BY n.nspname, t.typname;
            """)
            for schema, typname, labels in cursor.fetchall():
                formatted_labels = ", ".join(f"'{lbl}'" for lbl in labels)
                forward_ddl.append(f"CREATE TYPE {self._q(schema)}.{self._q(typname)} AS ENUM ({formatted_labels});")
                rollback_ddl.append(f"DROP TYPE IF EXISTS {self._q(schema)}.{self._q(typname)};")

            # -------------------------------------------------------------
            # 3. SEQUENCES
            # -------------------------------------------------------------
            write_line("\tExporting sequences...")
            cursor.execute("""
SELECT sequence_schema, sequence_name
FROM information_schema.sequences
WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema');
            """)
            for schema, seq_name in cursor.fetchall():
                forward_ddl.append(f"CREATE SEQUENCE IF NOT EXISTS {self._q(schema)}.{self._q(seq_name)};")
                rollback_ddl.append(f"DROP SEQUENCE IF EXISTS {self._q(schema)}.{self._q(seq_name)} CASCADE;")

            # -------------------------------------------------------------
            # 4. TABLES & COLUMNS
            # -------------------------------------------------------------
            write_line("\tExporting tables and columns...")
            cursor.execute("""
SELECT c.table_schema, c.table_name, c.column_name, c.data_type, c.udt_name,
    c.character_maximum_length, c.is_nullable, c.column_default
FROM information_schema.columns c
    JOIN information_schema.tables t ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE t.table_type = 'BASE TABLE' AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
            """)
            table_columns = defaultdict(list)
            for row in cursor.fetchall():
                (schema, table, col, dtype, udt_name, char_len, nullable, default) = row

                if char_len and dtype in ("character varying", "character"):
                    col_type = f"{dtype}({char_len})"
                elif dtype == "USER-DEFINED":
                    col_type = f"{self._q(schema)}.{self._q(udt_name)}"
                else:
                    col_type = dtype

                col_def = f"    {self._q(col)} {col_type}"
                if nullable == "NO":
                    col_def += " NOT NULL"
                if default is not None:
                    col_def += f" DEFAULT {default}"

                table_columns[(schema, table)].append(col_def)

            tables_list = []
            for (schema, table), cols in table_columns.items():
                if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                    continue
                cols_str = ",\n".join(cols)
                forward_ddl.append(f"CREATE TABLE IF NOT EXISTS {self._q(schema)}.{self._q(table)} (\n{cols_str}\n);")
                tables_list.append((schema, table))
            for schema, table in reversed(tables_list):
                rollback_ddl.append(f"DROP TABLE IF EXISTS {self._q(schema)}.{self._q(table)} CASCADE;")

            # -------------------------------------------------------------
            # 5. FUNCTIONS & PROCEDURES
            # -------------------------------------------------------------
            write_line("\tExporting functions and procedures...")
            cursor.execute("""
SELECT n.nspname, p.proname, pg_get_functiondef(p.oid)
FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND p.prokind IN ('f', 'p');
            """)
            for schema, name, func_def in cursor.fetchall():
                forward_ddl.append(f"{func_def};")
                # Extract argument signature for precise DROP FUNCTION matching
                cursor.execute("SELECT pg_get_function_identity_arguments(%s::regproc)", [f"{schema}.{name}"])
                args_sig = cursor.fetchone()
                if args_sig is None or len(args_sig) == 0:
                    raise ValueError(f"Could not extract argument signature for function {schema}.{name}")
                rollback_ddl.append(
                    f"DROP FUNCTION IF EXISTS {self._q(schema)}.{self._q(name)}({args_sig[0]}) CASCADE;"
                )

            # -------------------------------------------------------------
            # 6. VIEWS
            # -------------------------------------------------------------
            write_line("\tExporting views...")
            cursor.execute("""
SELECT table_schema, table_name, view_definition
FROM information_schema.views
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
            """)
            for schema, view_name, view_def in cursor.fetchall():
                forward_ddl.append(
                    f"CREATE OR REPLACE VIEW {self._q(schema)}.{self._q(view_name)} AS\n{view_def.strip()};"
                )
                rollback_ddl.append(f"DROP VIEW IF EXISTS {self._q(schema)}.{self._q(view_name)};")

            # -------------------------------------------------------------
            # 7. CONSTRAINTS (Primary Keys, Foreign Keys, Unique, Check)
            # Added via ALTER TABLE to avoid foreign key dependency ordering issues
            # -------------------------------------------------------------
            write_line("\tExporting constraints...")
            cursor.execute("""
SELECT n.nspname, c.relname, con.conname, pg_get_constraintdef(con.oid)
FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
            """)
            for schema, table, conname, condef in cursor.fetchall():
                if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                    continue
                forward_ddl.append(
                    f"ALTER TABLE ONLY {self._q(schema)}.{self._q(table)} ADD CONSTRAINT {self._q(conname)} {condef};"
                )
                # automatically drop constraints in DROP TABLE

            # -------------------------------------------------------------
            # 8. INDEXES (Excluding indexes created automatically by constraints)
            # -------------------------------------------------------------
            write_line("\tExporting indexes...")
            cursor.execute("""
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
  AND indexname NOT IN (SELECT conname FROM pg_constraint WHERE contype IN ('p', 'u'));
            """)
            for schema, table, indexname, indexdef in cursor.fetchall():
                if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                    continue
                forward_ddl.append(f"{indexdef};")
                # automatically drop indexes in DROP TABLE

            # -------------------------------------------------------------
            # 9. TRIGGERS
            # -------------------------------------------------------------
            write_line("\tExporting triggers...")
            cursor.execute("""
SELECT n.nspname, c.relname, trig.tgname, pg_get_triggerdef(trig.oid)
FROM pg_trigger trig
    JOIN pg_class c ON c.oid = trig.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND NOT trig.tgisinternal;
            """)
            for schema, table, tgname, tgdef in cursor.fetchall():
                if table.lower() == C.MIGRATEIT_MIGRATIONS_TABLE.lower():
                    continue
                forward_ddl.append(f"{tgdef};")
                # automatically drop triggers in DROP TABLE

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
            if "DROP COLUMN" in sql and "IF EXISTS" not in sql:
                return sql.replace("DROP COLUMN", "DROP COLUMN IF EXISTS", 1)
        return sql

    @override
    def _get_database_hash(self, migration_name: str) -> str:
        query = f"""
SELECT change_hash
FROM {self._q(self.table_name)}
WHERE migration_name = %s;
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query, (migration_name,))  # pyright: ignore
            result = cursor.fetchone()

            if not result or not result[0]:
                raise ValueError(f"Migration {migration_name} not found in the database")
            return result[0]

    def _update_migration_changelog(
        self,
        cursor: psycopg.Cursor,
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
        cursor.execute(query, (path.name, hash))  # pyright: ignore
