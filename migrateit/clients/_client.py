import hashlib
import os
import re
from abc import ABC
from pathlib import Path

from migrateit.clients._protocol import SqlClientProtocol
from migrateit.models import ChangelogFile, MigrateItConfig
from migrateit.tree import ROLLBACK_SPLIT_TAG

WHITESPACE_RE = re.compile(r"\s+")


class SqlClient[T](ABC, SqlClientProtocol):
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
        parts = content.split(ROLLBACK_SPLIT_TAG)
        if len(parts) == 1:
            raise ValueError("No rollback tag in migration file")
        if len(parts) > 2:
            raise ValueError("Too many rollback tags in migration file")

        migration, reverse_migration = parts
        return (
            WHITESPACE_RE.sub(" ", migration).strip(),
            WHITESPACE_RE.sub(" ", reverse_migration).strip(),
            hashlib.sha256(content.encode("utf-8")).hexdigest(),
        )
