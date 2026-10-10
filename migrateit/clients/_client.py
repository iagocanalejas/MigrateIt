from __future__ import annotations

import os
import re
from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, Any, override

import sqlfluff
from sqlfluff.core import Linter

from migrateit import constants as C
from migrateit.clients._protocol import SqlClientProtocol, SqlConnectionProtocol
from migrateit.models.changelog import ChangelogFile, SupportedDatabase
from migrateit.models.config import MigrateItConfig
from migrateit.models.migration import Migration, MigrationStatus, get_migration_header
from migrateit.models.sql import remove_sql_comments
from migrateit.reporters.logs import logger
from migrateit.reporters.output import write_line

if TYPE_CHECKING:
    from migrateit.models.connection import Connection


class SqlClient[T: Connection](ABC, SqlClientProtocol, SqlConnectionProtocol):
    VARNAME_DB_URL = os.getenv("VARNAME_DB_URL", "DB_URL")
    VARNAME_DB_FILE = os.getenv("VARNAME_DB_FILE", "DB_FILE")
    VARNAME_DB_HOST = os.getenv("VARNAME_DB_HOST", "DB_HOST")
    VARNAME_DB_PORT = os.getenv("VARNAME_DB_PORT", "DB_PORT")
    VARNAME_DB_USER = os.getenv("VARNAME_DB_USER", "DB_USER")
    VARNAME_DB_PASS = os.getenv("VARNAME_DB_PASS", "DB_PASS")
    VARNAME_DB_NAME = os.getenv("VARNAME_DB_NAME", "DB_NAME")
    VARNAME_DB_TIMEOUT_SECONDS = os.getenv("VARNAME_DB_TIMEOUT_SECONDS", "DB_TIMEOUT_SECONDS")

    connection: T
    config: MigrateItConfig

    @property
    @override
    def placeholder(self) -> str:
        return "%s"

    @property
    def table_name(self) -> str:
        return self.config.table_name

    @property
    def migrations_dir(self) -> Path:
        return self.config.migrations_dir

    @property
    def changelog(self) -> ChangelogFile:
        return self.config.changelog

    @classmethod
    def _q(cls, name: str, QUOTE_CHAR: str = '"') -> str:
        if QUOTE_CHAR in name or "\x00" in name:
            raise ValueError(f"Invalid identifier: {name!r}")
        return f"{QUOTE_CHAR}{name}{QUOTE_CHAR}"

    def __init__(self, connection: T, config: MigrateItConfig):
        if connection is None:
            raise ValueError("Database connection cannot be None")

        self.validate_config(config)

        self.connection = connection
        self.config = config

    @override
    def is_migration_applied(self, migration: Migration) -> bool:
        query = f"""
SELECT EXISTS (
    SELECT 1 FROM {self._q(self.table_name)} WHERE migration_name = {self.placeholder}
);
"""
        result = self.execute_for_one(query, (migration.name,))
        return bool(result[0]) if result else False

    @override
    def retrieve_migration_statuses(self) -> dict[str, MigrationStatus]:
        migrations = {k: MigrationStatus.NOT_APPLIED for k, _ in self.changelog.migrations_tree.items()}

        if not self.is_migrations_table_created():
            return migrations

        query = f"""
SELECT migration_name, change_hash
FROM {self._q(self.table_name)};
        """
        rows = self.execute_for_rows(query)

        migrations_by_name = {m.name: m for m in self.changelog.migrations}
        for row in rows:
            migration_name, db_hash = row
            migration = migrations_by_name.get(migration_name, None)
            if not migration:
                # migration applied not in changelog
                migrations[migration_name] = MigrationStatus.REMOVED
                continue

            _, _, migration_hash = migration.get_content_and_hash(self.migrations_dir)
            status = MigrationStatus.APPLIED
            if migration_hash != db_hash:
                status = MigrationStatus.CONFLICT
                write_line(f"Hash mismatch for {migration_name}: file={migration_hash} db={db_hash}")
                logger.warning("Hash mismatch for %s: file=%s db=%s", migration_name, migration_hash, db_hash)

            migrations[migration_name] = status

        return migrations

    @override
    def apply_migration(self, migration: Migration, is_fake: bool = False, is_rollback: bool = False) -> None:
        if not migration.initial and not (self.is_migration_applied(migration) == is_rollback):
            if is_rollback:
                raise ValueError(f"Migration {migration.name} is not applied, cannot undo it")
            raise ValueError(f"Migration {migration.name} is already applied, cannot apply it again")

        migration_code, reverse_migration_code, migration_hash = migration.get_content_and_hash(self.migrations_dir)

        code = migration_code if not is_rollback else reverse_migration_code
        if not is_fake and code.strip():
            parsed = Linter(dialect=self.changelog.database.value).parse_string(code)
            if len(parsed.violations) > 0:
                raise ValueError(parsed.violations)
            statements = [seg.raw.strip() for seg in parsed.tree.segments if seg.is_type("statement")]
            for stmt in statements:
                self.execute(stmt)
        self._update_migration_changelog(migration, migration_hash, is_rollback)

    @override
    def squash_migrations(self, migrations: list[str], new_migration: Migration) -> None:
        placeholders = ",".join(self.placeholder for _ in migrations)
        query = f"""
UPDATE {self._q(self.table_name)}
SET squashed = true
WHERE migration_name IN ({placeholders});
"""
        self.execute(query, tuple(migrations))
        self.apply_migration(new_migration, is_fake=True)

    @override
    def update_migration_hash(self, migration: Migration) -> None:
        _, _, migration_hash = migration.get_content_and_hash(self.migrations_dir)

        query = f"""
UPDATE {self._q(self.table_name)}
SET change_hash = {self.placeholder}
WHERE migration_name = {self.placeholder};
"""
        self.execute(query, (migration_hash, migration.name))

    # ---------------------------------------------------------------------------
    # Validation methods
    # ---------------------------------------------------------------------------

    @staticmethod
    def validate_config(config: MigrateItConfig) -> None:
        if not config.table_name:
            raise ValueError("Table name is required")
        if not isinstance(config.table_name, str):
            raise TypeError("Table name must be a string")
        if len(config.table_name) == 0:
            raise ValueError("Table name cannot be empty")
        if not config.table_name.isidentifier():
            raise ValueError("Table name must be a valid identifier")

        if not config.migrations_dir:
            raise ValueError("Migrations directory is required")
        if not config.changelog.path:
            raise ValueError("Migrations file is required")

    @override
    def validate_sql_syntax(self, migration: Migration) -> tuple[BaseException, str] | None:
        migration_code, reverse_migration_code, _ = migration.get_content_and_hash(self.migrations_dir)

        for code in (migration_code, reverse_migration_code):
            patched = self._patch_sql_statement(code)
            # Filter specifically for syntax/parsing errors
            dialect = self.changelog.database.value
            if dialect == SupportedDatabase.MARIADB.value:
                # NOTE: MariaDB uses same syntax as MySQL
                dialect = SupportedDatabase.MYSQL.value
            lint_errors = sqlfluff.lint(patched, dialect=dialect)
            syntax_errors = [e for e in lint_errors if e["code"] == "PRS"]

            if syntax_errors:
                err_msg = syntax_errors[0]["description"]
                return SyntaxError(f"SQL Syntax Error: {err_msg}"), code

        return None

    @override
    def validate_migrations(self, status_map: dict[str, MigrationStatus]) -> None:
        if len(self.changelog.migrations) == 0:
            return

        if not self.changelog.root.initial:
            raise ValueError("Initial migration is not defined in the changelog")
        if len([m for m in self.changelog.migrations if m.initial]) > 1:
            raise ValueError("Multiple initial migrations found in the changelog")

        # check removed migrations
        removed_migrations = [m for m, s in status_map.items() if s == MigrationStatus.REMOVED]
        if removed_migrations:
            raise ValueError(f"Removed migrations found in the database: {removed_migrations}. ")

        # check conflict migrations
        conflict_migrations = [m for m, s in status_map.items() if s == MigrationStatus.CONFLICT]
        if conflict_migrations:
            errors: list[str] = []
            for conflict_migration in conflict_migrations:
                migration = self.changelog.get_migration_by_name(conflict_migration)
                _, _, migration_hash = migration.get_content_and_hash(self.migrations_dir)
                errors.append(
                    f"Migration {conflict_migration} has a different hash in the database: "
                    f"found={migration_hash} existing={self._get_database_hash(conflict_migration)}"
                )
            raise ValueError("\n".join(errors))

        # check for each migration all the parents are applied
        for migration in self.changelog.migrations:
            if status_map[migration.name] != MigrationStatus.APPLIED:
                continue
            for parent in migration.parents:
                if status_map[parent] != MigrationStatus.APPLIED:
                    raise ValueError(f"Migration {migration.name} is applied before its parent {parent}.")

    # ---------------------------------------------------------------------------
    # Schema export
    # ---------------------------------------------------------------------------

    @override
    def export_database_schema(self, migration: Migration) -> None:
        if len(migration.parents) != 1 or migration.parents[0] != self.changelog.root.name:
            raise ValueError("Full database export must depend only on the initial migration")

        forward_ddl: list[str] = []
        rollback_ddl: list[str] = []

        write_line(f"Exporting full database schema to '{migration.name}'...")

        for item in self.export_items:
            write_line(f"\tExporting {item.name}...")
            rows = self.execute_for_rows(item.metadata_query, item.query_params)
            fwd, rb = item.process_rows(rows)
            forward_ddl.extend(fwd)
            rollback_ddl.extend(rb)

        migration_path = self.migrations_dir / migration.name
        with open(migration_path, "w", encoding="utf-8") as f:
            f.write(get_migration_header(migration_path))
            f.write("-- Migration automatically generated by migrateit\n\n")
            f.write("\n\n".join(forward_ddl) + "\n\n\n")
            f.write(C.ROLLBACK_SPLIT_TAG + "\n\n\n")
            f.write("\n\n".join(reversed(rollback_ddl)) + "\n")

    # ---------------------------------------------------------------------------
    # HELPERS
    # ---------------------------------------------------------------------------

    @override
    def _get_database_hash(self, migration_name: str) -> str:
        query = f"""
SELECT change_hash
FROM {self._q(self.table_name)}
WHERE migration_name = {self.placeholder};
"""
        result = self.execute_for_one(query, (migration_name,))
        if not result or not result[0]:
            raise ValueError(f"Migration {migration_name} not found in the database")
        return result[0]

    @override
    def _update_migration_changelog(self, migration: Migration, hash: str, is_rollback: bool) -> None:
        if migration.initial and is_rollback:
            return

        path = self.migrations_dir / migration.name
        if is_rollback and not migration.initial:
            query = f"""
DELETE FROM {self._q(self.table_name)}
WHERE migration_name = {self.placeholder}
    AND change_hash = {self.placeholder};
"""
        else:
            query = f"""
INSERT INTO {self._q(self.table_name)} (migration_name, change_hash)
VALUES ({self.placeholder}, {self.placeholder});
"""
        self.execute(query, (path.name, hash))

    @override
    def _patch_sql_statement(self, sql: str) -> str:
        sql = remove_sql_comments(sql)
        upper_sql = sql.upper()

        if "CREATE TABLE" in upper_sql and "IF NOT EXISTS" not in upper_sql:
            return re.sub("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", sql, flags=re.IGNORECASE)
        if "DROP TABLE" in upper_sql and "IF EXISTS" not in upper_sql:
            return re.sub("DROP TABLE", "DROP TABLE IF EXISTS", sql, flags=re.IGNORECASE)
        return sql


def get_client(config: MigrateItConfig, connection: Connection) -> SqlClient[Any]:
    match (config.changelog.database, connection):
        case (SupportedDatabase.MYSQL | SupportedDatabase.MARIADB, _ as mysql_conn):
            from .mysql import MySqlClient

            return MySqlClient(mysql_conn, config)  # type: ignore[arg-type]
        case (SupportedDatabase.POSTGRES, _ as pg_conn):
            from .psql import PsqlClient

            return PsqlClient(pg_conn, config)  # type: ignore[arg-type]
        case (SupportedDatabase.SQLITE, _ as sq_conn):
            from .sqlite import SqliteClient

            return SqliteClient(sq_conn, config)  # type: ignore[arg-type]
        case _:
            raise NotImplementedError(f"Database {config.changelog.database} is not supported")
