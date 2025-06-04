import json
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from .migration import Migration


class SupportedDatabase(Enum):
    POSTGRES = "postgres"


@dataclass
class ChangelogFile:
    version: int
    database: SupportedDatabase = SupportedDatabase.POSTGRES
    migrations: list[Migration] = field(default_factory=list)
    path: Path = field(default_factory=Path)

    def __str__(self) -> str:
        return self.path.name

    def __repr__(self) -> str:
        return str(self)

    @staticmethod
    def from_json(json_str: str, file_path: Path) -> "ChangelogFile":
        data = json.loads(json_str)
        try:
            migrations = [Migration(**m) for m in data.get("migrations", [])]
            return ChangelogFile(
                version=data["version"],
                database=SupportedDatabase(data.get("database", SupportedDatabase.POSTGRES.value)),
                migrations=migrations,
                path=file_path,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"Invalid JSON for MigrationsFile: {e}")

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "database": self.database.value,
            "migrations": [migration.to_dict() for migration in self.migrations],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=4)

    def exist_migration_by_name(self, name: str) -> bool:
        name = os.path.basename(name) if os.path.isabs(name) else name
        prefix = name.split("_", 1)[0]
        return any(m.name.startswith(prefix) for m in self.migrations)

    def get_migration_by_name(self, name: str) -> Migration:
        if os.path.isabs(name):
            name = os.path.basename(name)
        name = name.split("_")[0]  # get the migration number
        for migration in self.migrations:
            if migration.name.startswith(name):
                return migration

        raise ValueError(f"Migration '{name}' not found in changelog")
