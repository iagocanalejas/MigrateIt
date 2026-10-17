from __future__ import annotations

import functools
import json
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from migrateit.models.config import SupportedDatabase
from migrateit.models.migration import Migration


@dataclass(kw_only=True)
class Changelog:
    version: int
    database: SupportedDatabase = SupportedDatabase.POSTGRES
    migrations: list[Migration] = field(default_factory=list)
    path: Path = field(default_factory=Path)

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

    def __str__(self) -> str:  # pragma: no cover
        return self.path.name

    def __repr__(self) -> str:  # pragma: no cover
        return str(self)

    @staticmethod
    def from_json(json_str: str, file_path: Path) -> Changelog:
        data = json.loads(json_str)
        try:
            migrations = [Migration(**m) for m in data.get("migrations", [])]
            return Changelog(
                version=data["version"],
                database=SupportedDatabase(data.get("database", SupportedDatabase.POSTGRES.value)),
                migrations=migrations,
                path=file_path,
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
