import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class MigrationStatus(Enum):
    APPLIED = "applied"
    CONFLICT = "conflict"
    REMOVED = "removed"
    NOT_APPLIED = "not_applied"


@dataclass
class Migration:
    name: str
    initial: bool = False
    parents: list[str] = field(default_factory=list)

    @staticmethod
    def is_valid_name(path: Path) -> bool:
        return path.is_file() and path.name.endswith(".sql") and re.match(r"^\d{4}_", path.name) is not None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "initial": self.initial,
            "parents": self.parents,
        }
