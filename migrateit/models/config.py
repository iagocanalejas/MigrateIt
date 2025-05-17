from dataclasses import dataclass
from pathlib import Path

from .changelog import ChangelogFile


@dataclass
class MigrateItConfig:
    table_name: str
    migrations_dir: Path
    changelog: ChangelogFile
