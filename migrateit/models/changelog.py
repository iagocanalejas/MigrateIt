import functools
import json
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from migrateit import constants as C
from migrateit.models.config import SupportedDatabase
from migrateit.models.migration import Migration


@dataclass(kw_only=True)
class Changelog:
    version: int
    database: SupportedDatabase
    migrations: list[Migration] = field(default_factory=list)

    path = C.MIGRATEIT_CHANGELOG_FILE

    def __str__(self) -> str:  # pragma: no cover
        return self.path.name

    def __repr__(self) -> str:  # pragma: no cover
        return str(self)

    @functools.cached_property
    def root(self) -> Migration:
        if not self.migrations:
            raise ValueError("No migrations found. Changelog is not initialized.")
        if not self.migrations[0].initial:
            raise ValueError("Initial migration is not defined in the changelog")
        return self.migrations[0]

    @functools.cached_property
    def migrations_tree(self) -> dict[str, list[Migration]]:
        d: dict[str, list[Migration]] = OrderedDict[str, list[Migration]]()
        for migration in self.migrations:
            if migration.name in d:
                raise ValueError(f"Found duplicated migration={migration.name}")
            d[migration.name] = []
            for parent in migration.parents:
                d[parent].append(migration)
        return d

    @staticmethod
    def from_json() -> Changelog:
        if not Changelog.path.exists():
            raise FileNotFoundError(f"File {Changelog.path.name} does not exist")
        data = json.loads(Changelog.path.read_text())
        try:
            migrations = [Migration(**m) for m in data.get("migrations", [])]
            return Changelog(
                version=data["version"],
                database=SupportedDatabase(data.get("database", SupportedDatabase.POSTGRES.value)),
                migrations=migrations,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"Invalid JSON for MigrationsFile: {e}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "database": self.database.value,
            "migrations": [migration.to_dict() for migration in self.migrations],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=4)
