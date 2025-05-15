from abc import ABC
from pathlib import Path

from migrateit.clients._protocol import SqlClientProtocol
from migrateit.models import ChangelogFile, MigrateItConfig


class SqlClient[T](ABC, SqlClientProtocol):
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
        assert connection is not None, "Database connection is required"

        self.validate_config(config)

        self.connection = connection
        self.config = config

    @staticmethod
    def validate_config(config: MigrateItConfig) -> None:
        assert config.table_name, "Table name is required"
        assert isinstance(config.table_name, str), "Table name must be a string"
        assert len(config.table_name) > 0, "Table name cannot be empty"
        assert config.table_name.isidentifier(), "Table name must be a valid identifier"

        assert config.migrations_dir, "Migrations directory is required"
        assert config.changelog.path, "Migrations file is required"
