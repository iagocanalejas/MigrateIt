import json
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from migrateit.reporters.output import STATUS_COLORS, write_line

from .migration import Migration, MigrationStatus


class SupportedDatabase(Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"


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

    def to_dict(self) -> dict[str, Any]:
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
            if migration.name.split("_")[0] == name:
                return migration

        raise ValueError(f"Migration '{name}' not found in changelog")

    def print_dag(self, status_map: dict[str, MigrationStatus]) -> None:
        from migrateit.tree import build_migrations_tree

        migration_tree = build_migrations_tree(self)
        first_migration = next(iter(migration_tree))
        ChangelogFile._print_dag_rec(first_migration, migration_tree, status_map)

    def print_list(self, status_map: dict[str, MigrationStatus]) -> None:
        from migrateit.tree import build_migrations_tree

        migration_tree = build_migrations_tree(self)
        for name in migration_tree.keys():
            status = status_map[name]
            status_str = f"{STATUS_COLORS[status]}{status.name.replace('_', ' ').title()}{STATUS_COLORS['reset']}"
            write_line(f"{name:<40} | {status_str}")

    @staticmethod
    def _print_dag_rec(
        name: str,
        children: dict[str, list[Migration]],
        status_map: dict[str, MigrationStatus],
        level: int = 0,
        seen: set[str] = set(),
    ) -> None:
        indent = "  " * level + ("└─ " if level > 0 else "")
        status = status_map[name]
        status_str = f"{STATUS_COLORS[status]}{status.name.replace('_', ' ').title()}{STATUS_COLORS['reset']}"

        # indicate repeated visit
        repeat_marker = " (*)" if name in seen else ""
        write_line(f"{indent}{name:<40} | {status_str}{repeat_marker}")

        if name in seen:
            return
        seen.add(name)

        for child in children.get(name, []):
            ChangelogFile._print_dag_rec(child.name, children, status_map, level + 1, seen)
