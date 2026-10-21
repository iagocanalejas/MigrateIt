from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from migrateit import constants as C

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
    changelog: Changelog
    table_name: str = field(init=False)

    def __post_init__(self) -> None:
        self.table_name = C.MIGRATEIT_MIGRATIONS_TABLE

        if not isinstance(self.table_name, str):
            raise TypeError("Table name must be a string")
        if not self.table_name.strip():
            raise ValueError("Table name is required")
        if not self.table_name.isidentifier():
            raise ValueError("Table name must be a valid identifier")

        if not self.changelog or not self.changelog.path:
            raise ValueError("Migrations file is required")
