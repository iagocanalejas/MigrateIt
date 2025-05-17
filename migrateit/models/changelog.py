import json
import os
from collections import defaultdict
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

    @property
    def graph(self) -> tuple[str, dict[str, list[str]]]:
        """
        Build a graph of migrations and their dependencies.
        Returns:
            A tuple containing the root migration name and a dictionary of children migrations.
        """
        root = None
        children = defaultdict(list)
        for migration in self.migrations:
            if migration.initial:
                assert root is None, "Multiple initial migrations found"
                root = migration.name
            for parent in migration.parents:
                children[parent].append(migration.name)
        assert root is not None, "No initial migration found"
        return root, children

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

    def get_migration_by_name(self, name: str) -> Migration:
        if os.path.isabs(name):
            name = os.path.basename(name)
        name = name.split("_")[0]  # get the migration number
        for migration in self.migrations:
            if migration.name.startswith(name):
                return migration
        raise ValueError(f"Migration {name} not found in changelog")
