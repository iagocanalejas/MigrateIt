from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .changelog import Changelog


class SupportedDatabase(Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    MYSQL = "mysql"
    MARIADB = "mariadb"
    ORACLE = "oracle"


@dataclass(kw_only=True)
class MigrateItConfig:
    table_name: str
    migrations_dir: Path
    changelog: Changelog

    @staticmethod
    def validate(config: MigrateItConfig) -> None:
        if not isinstance(config.table_name, str):
            raise TypeError("Table name must be a string")
        if not config.table_name.strip():
            raise ValueError("Table name is required")
        if not config.table_name.isidentifier():
            raise ValueError("Table name must be a valid identifier")

        if not config.migrations_dir:
            raise ValueError("Migrations directory is required")
        if not config.changelog or not config.changelog.path:
            raise ValueError("Migrations file is required")
