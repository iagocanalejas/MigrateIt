import os
from abc import ABC
from pathlib import Path

from migrateit.clients._protocol import SqlClientProtocol
from migrateit.models import ChangelogFile, MigrateItConfig


class SqlClient[T](ABC, SqlClientProtocol):
    VARNAME_DB_URL = os.getenv("VARNAME_DB_URL", "DB_URL")
    VARNAME_DB_HOST = os.getenv("VARNAME_DB_HOST", "DB_HOST")
    VARNAME_DB_PORT = os.getenv("VARNAME_DB_PORT", "DB_PORT")
    VARNAME_DB_USER = os.getenv("VARNAME_DB_USER", "DB_USER")
    VARNAME_DB_PASS = os.getenv("VARNAME_DB_PASS", "DB_PASS")
    VARNAME_DB_NAME = os.getenv("VARNAME_DB_NAME", "DB_NAME")

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
