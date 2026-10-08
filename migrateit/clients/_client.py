import hashlib
import os
import re
from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, Any, override

import sqlfluff
from sqlfluff.core import Linter

from migrateit import constants as C
from migrateit.clients._protocol import SqlClientProtocol
from migrateit.models.changelog import ChangelogFile, SupportedDatabase
from migrateit.models.config import MigrateItConfig
from migrateit.models.migration import Migration, MigrationStatus

WHITESPACE_RE = re.compile(r"\s+")


if TYPE_CHECKING:
    from migrateit.models.connection import Connection


class SqlClient[T: Connection](ABC, SqlClientProtocol):
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
    def table_name(self) -> str:
        return self.config.table_name

    @property
    def migrations_dir(self) -> Path:
        return self.config.migrations_dir

    @property
    def changelog(self) -> ChangelogFile:
        return self.config.changelog

    def __init__(self, connection: T, config: MigrateItConfig):
        if connection is None:
            raise ValueError("Database connection cannot be None")

        self.validate_config(config)

        self.connection = connection
        self.config = config

    @classmethod
    def _q(cls, name: str, QUOTE_CHAR: str = '"') -> str:
        if QUOTE_CHAR in name or "\x00" in name:
            raise ValueError(f"Invalid identifier: {name!r}")
        return f"{QUOTE_CHAR}{name}{QUOTE_CHAR}"

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

    @staticmethod
    def get_migration_content_and_hash(path: Path) -> tuple[str, str, str]:
        content = path.read_text()
        parts = content.split(C.ROLLBACK_SPLIT_TAG)
        if len(parts) == 1:
            raise ValueError("No rollback tag in migration file")
        if len(parts) > 2:
            raise ValueError("Too many rollback tags in migration file")

        migration = SqlClient._remove_sql_comments(parts[0])
        reverse_migration = SqlClient._remove_sql_comments(parts[1])
        return (
            WHITESPACE_RE.sub(" ", migration).strip(),
            WHITESPACE_RE.sub(" ", reverse_migration).strip(),
            hashlib.sha256(content.encode("utf-8")).hexdigest(),
        )

    @staticmethod
    def _remove_sql_comments(sql: str) -> str:
        linter = Linter(dialect="ansi")
        parsed = linter.parse_string(sql)
        return "".join(segment.raw for segment in parsed.tree.raw_segments if not segment.is_type("comment")).strip()

    def get_migration_path(self, migration: Migration) -> Path:
        path = self.migrations_dir / migration.name
        if not path.is_file() or not path.name.endswith(".sql"):
            raise FileNotFoundError(f"Migration file {path.name} does not exist or is not a valid SQL file")
        return path

    @override
    def validate_sql_syntax(self, migration: Migration) -> tuple[BaseException, str] | None:
        path = self.get_migration_path(migration)
        migration_code, reverse_migration_code, _ = self.get_migration_content_and_hash(path)

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
                path = self.migrations_dir / conflict_migration
                _, _, migration_hash = self.get_migration_content_and_hash(path)
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

    @override
    def _patch_sql_statement(self, sql: str) -> str:
        sql = self._remove_sql_comments(sql.upper())

        if not any(w in sql for w in ("CREATE ", "ALTER ", "DROP ")):
            return sql
        if "CREATE TABLE" in sql and "IF NOT EXISTS" not in sql:
            return sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
        if "DROP TABLE" in sql and "IF EXISTS" not in sql:
            return sql.replace("DROP TABLE", "DROP TABLE IF EXISTS", 1)
        return sql


def get_client(config: MigrateItConfig, connection: "Connection") -> SqlClient[Any]:
    match (config.changelog.database, connection):
        case (SupportedDatabase.MYSQL | SupportedDatabase.MARIADB, _ as mysql_conn):
            from .mysql import MySqlClient

            return MySqlClient(mysql_conn, config)  # type: ignore
        case (SupportedDatabase.POSTGRES, _ as pg_conn):
            from .psql import PsqlClient

            return PsqlClient(pg_conn, config)  # type: ignore
        case (SupportedDatabase.SQLITE, _ as sq_conn):
            from .sqlite import SqliteClient

            return SqliteClient(sq_conn, config)  # type: ignore
        case _:
            raise NotImplementedError(f"Database {config.changelog.database} is not supported")
