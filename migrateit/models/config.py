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


@dataclass(kw_only=True)
class MigrateItConfig:
    table_name: str
    migrations_dir: Path
    changelog: Changelog
