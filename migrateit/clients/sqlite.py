import os
import re
import sqlite3
from typing import Any, override

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.models.migration import Migration, MigrationStatus, get_migration_header
from migrateit.reporters.logs import logger
from migrateit.reporters.output import write_line


def _q(name: str) -> str:
    """Wrap a SQLite identifier in double quotes (safe: rejects double quotes and null bytes)."""
    if '"' in name or "\x00" in name:
        raise ValueError(f"Invalid SQLite identifier: {name!r}")
    return f'"{name}"'


def _split_sql_statements(sql: str) -> list[str]:
    """Split SQL into individual statements, respecting semicolons inside strings."""
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            current.append(char)
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(char)
        elif char == ";" and not in_single_quote and not in_double_quote:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(char)
        i += 1
    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


class SqliteClient(SqlClient[sqlite3.Connection]):
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
CREATE TABLE IF NOT EXISTS {table_name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_name VARCHAR(255) UNIQUE NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_hash VARCHAR(64) NOT NULL,
    squashed BOOLEAN DEFAULT 0
);
        """
        reverse_query = f"""
DROP TABLE IF EXISTS {table_name};
        """
        return migrations_query, reverse_query

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
    def is_migration_applied(self, migration: Migration) -> bool:
        """Check if a migration has been applied in SQLite."""
        query = f"""
SELECT EXISTS(
    SELECT 1 FROM {self.table_name} WHERE migration_name=?
);
"""
        cursor = self.connection.execute(query, (migration.name,))
        result = cursor.fetchone()
        return bool(result[0]) if result else False

    @override
    def retrieve_migration_statuses(self) -> dict[str, MigrationStatus]:
        """Retrieve migration statuses from the SQLite database."""

        migrations = {k: MigrationStatus.NOT_APPLIED for k, _ in self.changelog.migrations_tree.items()}

        if not self.is_migrations_table_created():
            return migrations

        query = f"""
SELECT migration_name, change_hash
FROM {self.table_name};
"""
        cursor = self.connection.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            migration_name = row[0]
            db_hash = row[1]
            migration = next((m for m in self.changelog.migrations if m.name == migration_name), None)
            if not migration:
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
        if is_fake and is_rollback:
            raise ValueError("Cannot fake a rollback migration")

        path = self.get_migration_path(migration)
        if not migration.initial and not (self.is_migration_applied(migration) == is_rollback):
            if is_rollback:
                raise ValueError(f"Migration {path.name} is not applied, cannot undo it")
            raise ValueError(f"Migration {path.name} is already applied, cannot apply it again")

        migration_code, reverse_migration_code, migration_hash = self.get_migration_content_and_hash(path)

        try:
            code = migration_code if not is_rollback else reverse_migration_code
            if not is_fake and code.strip():
                # NOTE: avoid executescript() which implicitly commits and cannot be rolled back.
                statements = _split_sql_statements(code)
                for stmt in statements:
                    self.connection.execute(stmt)
            self._update_migration_changelog(migration, migration_hash, is_rollback)
        except sqlite3.Error as e:
            self.connection.rollback()
            raise e

    @override
    def squash_migrations(self, migrations: list[str], new_migration: Migration) -> None:
        """Mark migrations as squashed in SQLite."""
        placeholders = ",".join("?" for _ in migrations)
        query = f"""
UPDATE {self.table_name} SET squashed=1
WHERE migration_name IN ({placeholders});
"""
        with self.connection:
            self.connection.execute(query, migrations)
        self.apply_migration(new_migration, is_fake=True)

    @override
    def update_migration_hash(self, migration: Migration) -> None:
        """Update the hash of a migration in SQLite."""
        path = self.get_migration_path(migration)
        _, _, migration_hash = self.get_migration_content_and_hash(path)

        query = f"""
INSERT OR REPLACE INTO {self.table_name} (migration_name, change_hash)
VALUES (?, ?);
"""
        cursor = self.connection.execute(query, (path.name, migration_hash))
        cursor.fetchall()

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
            rollback_ddl.append(f"DROP TABLE IF EXISTS {_q(name)};")

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
            rollback_ddl.append(f"DROP VIEW IF EXISTS {_q(name)};")

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

    @override
    def validate_migrations(self, status_map: dict[str, MigrationStatus]) -> None:
        """Validate migration statuses for SQLite."""
        if len(self.changelog.migrations) == 0:
            return

        if not self.changelog.root.initial:
            raise ValueError("Initial migration is not defined in the changelog")
        if len([m for m in self.changelog.migrations if m.initial]) > 1:
            raise ValueError("Multiple initial migrations found in the changelog")

        removed_migrations = [m for m, s in status_map.items() if s == MigrationStatus.REMOVED]
        if removed_migrations:
            raise ValueError(f"Removed migrations found in the database: {removed_migrations}. ")

        conflict_migrations = [m for m, s in status_map.items() if s == MigrationStatus.CONFLICT]
        if conflict_migrations:
            errors: list[str] = []
            for conflict_migration in conflict_migrations:
                path = self.migrations_dir / conflict_migration
                _, _, migration_hash = self.get_migration_content_and_hash(path)
                errors.append(
                    f"Migration {conflict_migration} has a different hash in the database: "
                    f"found={migration_hash} existing={self._get_database_hash(conflict_migration)}"
                )
            raise ValueError("\n".join(errors))

        for migration in self.changelog.migrations:
            if status_map[migration.name] != MigrationStatus.APPLIED:
                continue
            for parent in migration.parents:
                if status_map[parent] != MigrationStatus.APPLIED:
                    raise ValueError(f"Migration {migration.name} is applied before its parent {parent}.")

    def _update_migration_changelog(self, migration: Migration, hash: str, is_rollback: bool) -> None:
        """Insert or delete a migration record in SQLite."""
        if is_rollback and not migration.initial:
            query = f"""
DELETE FROM {self.table_name}
WHERE migration_name=?
    AND change_hash=?;
"""
        else:
            query = f"""
INSERT INTO {self.table_name} (migration_name, change_hash)
VALUES (?, ?);
"""
        self.connection.execute(query, ((self.migrations_dir / migration.name).name, hash))

    def _get_database_hash(self, migration_name: str) -> str:
        """Retrieve a migration's hash from the SQLite database."""
        query = f"""
SELECT change_hash
FROM {self.table_name}
WHERE migration_name=?;
"""
        cursor = self.connection.execute(query, (migration_name,))
        result = cursor.fetchone()

        if not result or not result[0]:
            raise ValueError(f"Migration {migration_name} not found in the database")
        return result[0]
