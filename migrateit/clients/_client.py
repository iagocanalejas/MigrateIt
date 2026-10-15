from __future__ import annotations

import functools
import os
from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, Any, override

import sqlfluff
from sqlfluff.core import Linter

from migrateit import constants as C
from migrateit.clients._protocol import SqlClientProtocol, SqlConnectionProtocol
from migrateit.models.changelog import Changelog
from migrateit.models.config import MigrateItConfig, SupportedDatabase
from migrateit.models.migration import Migration, MigrationStatus, get_migration_header
from migrateit.models.migration_manager import MigrationManager
from migrateit.models.sql import patch_sql_statement
from migrateit.reporters.logs import logger
from migrateit.reporters.output import write_line

if TYPE_CHECKING:
    from migrateit.models.connection import Connection


class SqlClient[T: Connection](ABC, SqlClientProtocol, SqlConnectionProtocol):
    VARNAME_DB_URL = os.getenv("VARNAME_DB_URL", "DB_URL")
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
    def changelog(self) -> Changelog:
        return self.config.changelog

    @property
    def database(self) -> SupportedDatabase:
        return self.config.changelog.database

    @property
    def migrations(self) -> MigrationManager:
        return MigrationManager(self.config.changelog)

    @functools.cached_property
    def _linter(self) -> Linter:
        return Linter(dialect=self.database.value)

    @classmethod
    def _q(cls, name: str, QUOTE_CHAR: str = '"') -> str:
        if QUOTE_CHAR in name or "\x00" in name:
            raise ValueError(f"Invalid identifier: {name!r}")
        return f"{QUOTE_CHAR}{name}{QUOTE_CHAR}"

    def __init__(self, connection: T, config: MigrateItConfig):
        if connection is None:
            raise ValueError("Database connection cannot be None")

        MigrateItConfig.validate(config)

        self.connection = connection
        self.config = config

    @override
    def is_migration_applied(self, migration: Migration) -> bool:
        query = f"""
SELECT COUNT(*)
FROM {self._q(self.table_name)}
WHERE migration_name = {self.placeholder};
"""
        result = self.execute_for_one(query, (migration.name,))
        return result[0] > 0 if result else False

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
        is_applied = self.is_migration_applied(migration) if not migration.initial else False
        if not migration.initial and is_applied != is_rollback:
            if is_rollback:
                raise ValueError(f"Migration {migration.name} is not applied, cannot undo it")
            raise ValueError(f"Migration {migration.name} is already applied, cannot apply it again")

        migration_code, reverse_migration_code, migration_hash = migration.get_content_and_hash(self.migrations_dir)

        code = migration_code if not is_rollback else reverse_migration_code
        if not is_fake and code.strip():
            parsed = self._linter.parse_string(code)
            if len(parsed.violations) > 0:
                raise ValueError(parsed.violations)
            statements = [seg.raw.strip() for seg in parsed.tree.recursive_crawl("statement")]
            assert len(statements) > 0, "No statements found in migration code"
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

    @override
    def validate_sql_syntax(self, migration: Migration) -> tuple[BaseException, str] | None:
        migration_code, reverse_migration_code, _ = migration.get_content_and_hash(self.migrations_dir)
        # Filter specifically for syntax/parsing errors
        dialect = self.database
        if dialect == SupportedDatabase.MARIADB:  # pragma: no cover
            # NOTE: MariaDB uses same syntax as MySQL
            dialect = SupportedDatabase.MYSQL

        for code in (migration_code, reverse_migration_code):
            patched = patch_sql_statement(dialect, code)
            lint_errors = sqlfluff.lint(patched, dialect=dialect.value)
            syntax_errors = [e for e in lint_errors if e["code"] == "PRS"]

            if syntax_errors:
                err_msg = syntax_errors[0]["description"]
                return SyntaxError(f"SQL Syntax Error: {err_msg}"), code

        return None

    @override
    def validate_migrations(self, status_map: dict[str, MigrationStatus]) -> None:
        if len(self.changelog.migrations) == 0:
            return
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
                migration = self.migrations.get_migration_by_name(conflict_migration)
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


def get_client(config: MigrateItConfig, connection: Connection) -> SqlClient[Any]:
    if config.changelog.database in [SupportedDatabase.MYSQL, SupportedDatabase.MARIADB]:
        from .mysql import MySqlClient

        return MySqlClient(connection, config)  # type: ignore[arg-type]
    if config.changelog.database == SupportedDatabase.ORACLE:
        from .oracle import OracleClient

        return OracleClient(connection, config)  # type: ignore[arg-type]
    if config.changelog.database == SupportedDatabase.POSTGRES:
        from .psql import PsqlClient

        return PsqlClient(connection, config)  # type: ignore[arg-type]
    if config.changelog.database == SupportedDatabase.SQLITE:
        from .sqlite import SqliteClient

        return SqliteClient(connection, config)  # type: ignore[arg-type]
    raise NotImplementedError(f"Database {config.changelog.database} is not supported")
