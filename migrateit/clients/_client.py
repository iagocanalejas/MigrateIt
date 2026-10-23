from __future__ import annotations

import functools
import os
import re
from abc import ABC
from typing import TYPE_CHECKING, Any, override

from sqlfluff.core import Linter, SQLBaseError

from migrateit import constants as C
from migrateit.clients._protocol import SqlClientProtocol, SqlConnectionProtocol
from migrateit.models.changelog import Changelog
from migrateit.models.config import MigrateItConfig, SupportedDatabase
from migrateit.models.migration import Migration, MigrationStatus, get_migration_header
from migrateit.models.migration_manager import MigrationManager
from migrateit.models.sql import patch_sql_statement, sql_to_statements
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

            status = MigrationStatus.APPLIED
            if migration.hash != db_hash:
                status = MigrationStatus.CONFLICT
                write_line(f"Hash mismatch for {migration_name}: file={migration.hash} db={db_hash}")
                logger.warning("Hash mismatch for %s: file=%s db=%s", migration_name, migration.hash, db_hash)

            migrations[migration_name] = status

        return migrations

    @override
    def apply_migration(self, migration: Migration, is_fake: bool = False, is_rollback: bool = False) -> None:
        is_applied = self.is_migration_applied(migration) if not migration.initial else False
        if not migration.initial and is_applied != is_rollback:
            if is_rollback:
                raise ValueError(f"Migration {migration.name} is not applied, cannot undo it")
            raise ValueError(f"Migration {migration.name} is already applied, cannot apply it again")

        migration_code, reverse_migration_code = migration.content
        code = migration_code if not is_rollback else reverse_migration_code
        if not is_fake and code.strip():
            statements, errors = sql_to_statements(self._linter, code)
            if errors:
                raise ExceptionGroup(
                    "Multiple SQL linter errors detected in query",
                    self._build_error_message(migration, errors),
                )
            assert len(statements) > 0, "No statements found in migration code"
            for stmt in statements:
                self.execute(patch_sql_statement(self.database, stmt))
        self._update_migration_changelog(migration, is_rollback)

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
        query = f"""
UPDATE {self._q(self.table_name)}
SET change_hash = {self.placeholder}
WHERE migration_name = {self.placeholder};
"""
        self.execute(query, (migration.hash, migration.name))

    # ---------------------------------------------------------------------------
    # Validation methods
    # ---------------------------------------------------------------------------

    @override
    def validate_sql_syntax(self, migration: Migration) -> list[tuple[SyntaxError, str]]:
        migration_code, rb_migration_code = migration.content
        _, migration_errors = sql_to_statements(self._linter, migration_code)
        _, rb_migration_errors = sql_to_statements(self._linter, rb_migration_code)

        errors = [(e, migration_code) for e in self._build_error_message(migration, migration_errors)]
        rb_errors = [(e, rb_migration_code) for e in self._build_error_message(migration, rb_migration_errors)]
        return errors + rb_errors

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
                errors.append(
                    f"Migration {conflict_migration} has a different hash in the database: "
                    f"found={migration.hash} existing={self._get_database_hash(conflict_migration)}"
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

        with open(migration.path, "w", encoding="utf-8") as f:
            f.write(get_migration_header(migration.path))
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
    def _update_migration_changelog(self, migration: Migration, is_rollback: bool) -> None:
        if migration.initial and is_rollback:
            return

        if is_rollback and not migration.initial:
            query = f"""
DELETE FROM {self._q(self.table_name)}
WHERE migration_name = {self.placeholder}
    AND change_hash = {self.placeholder};
"""
        else:
            # NOTE: this will raise an expected error if the migration was already applied.
            query = f"""
INSERT INTO {self._q(self.table_name)} (migration_name, change_hash)
VALUES ({self.placeholder}, {self.placeholder});
"""
        self.execute(query, (migration.path.name, migration.hash))

    def _build_error_message(self, migration: Migration, errors: list[SQLBaseError]) -> list[SyntaxError]:
        lines = migration.path.read_text(encoding="utf-8").splitlines(keepends=True)
        lines = [line.strip() for line in lines]

        processed = []
        for error in errors:
            if not error.description:
                processed.append(SyntaxError(f"{migration}: SQL Syntax Error (unknown location)"))
                continue
            assert error.description is not None
            matches = re.findall(r"'(.*?)'", error.description)
            if not matches:
                processed.append(SyntaxError(f"{migration}: SQL Syntax Error: {error.description}"))
                continue
            src = matches[0]
            try:
                idx = next(i + 1 for i, line in enumerate(lines) if src in line)
            except StopIteration:
                idx = 1
            err = SyntaxError(f"{migration}: Line {idx}, Position {error.line_pos}: SQL Syntax Error: '{src}'")
            processed.append(err)
        return processed


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
