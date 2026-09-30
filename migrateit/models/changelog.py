import json
import os
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from migrateit.reporters.output import STATUS_COLORS, write_line

from .migration import Migration, MigrationStatus


class SupportedDatabase(Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    MYSQL = "mysql"


@dataclass
class ChangelogFile:
    version: int
    database: SupportedDatabase = SupportedDatabase.POSTGRES
    migrations: list[Migration] = field(default_factory=list)
    path: Path = field(default_factory=Path)

    @property
    def migrations_tree(self) -> OrderedDict[str, list[Migration]]:
        d = OrderedDict[str, list[Migration]]()
        for migration in self.migrations:
            if migration.name in d:
                raise ValueError(f"Found duplicated migration={migration.name}")
            d[migration.name] = []
            for parent in migration.parents:
                d[parent].append(migration)
        return d

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

    def build_migration_plan(
        self,
        statuses_map: dict[str, MigrationStatus],
        target_migration: Migration | None = None,
        is_rollback: bool = False,
    ) -> list[Migration]:
        """
        Build a migration plan based on the changelog and migration tree.
        Args:
            statuses_map: A map of migration names to their statuses.
            target_migration: The target migration to apply or rollback to.
            is_rollback: Whether the plan is for a rollback operation.
        Returns:
            A list of migrations to apply or rollback, in the correct order.
        """
        plan: list[Migration] = []
        visited: set[str] = set()
        queue: deque[Migration] = deque([self.migrations[0]])
        is_bottom_up = target_migration is not None and not is_rollback
        is_normal_order = not is_bottom_up and not is_rollback

        if is_rollback:
            if not target_migration:
                raise ValueError("Target migration is required for rollback plan")
            queue = deque([target_migration])

        if is_bottom_up:
            if not target_migration:
                raise ValueError("Target migration is required for bottom-up plan")
            queue = deque([target_migration])

        def get_neighbors(m: Migration) -> list[str]:
            # get the children of the migration
            return list(reversed(m.parents)) if is_bottom_up else [m.name for m in self.migrations_tree.get(m.name, [])]

        while queue:
            current = queue.popleft()

            has_unvisited_parents = any(p not in visited for p in current.parents)
            if is_normal_order and has_unvisited_parents:
                # Structure: A → B, A → D, B → C, C → D
                # Tree: { A: [B, D], B: [C], C: [D], D: [] }
                # D will be processed before it's parent C, so we skip and requeue it
                queue.append(current)
                continue

            visited.add(current.name)
            plan.append(current)
            for neighbor_name in get_neighbors(current):
                neighbor = self.get_migration_by_name(neighbor_name)
                if neighbor.name not in visited and neighbor not in queue:
                    # Structure: A → B, A → C, B → C
                    # Tree: { A: [B, C], B: [C], C: [] }
                    # when: is_bottom_up=True
                    # C is a common child of A and B. It gets added to the queue twice, so we skip the second visit.
                    queue.append(neighbor)

        plan = list(reversed(plan)) if not is_normal_order else plan
        if is_rollback:
            return [p for p in plan if statuses_map[p.name] == MigrationStatus.APPLIED]
        return [p for p in plan if statuses_map[p.name] != MigrationStatus.APPLIED]

    def print_list(self, status_map: dict[str, MigrationStatus]) -> None:
        migration_tree = self.migrations_tree
        for name in migration_tree.keys():
            status = status_map[name]
            status_str = f"{STATUS_COLORS[status]}{status.name.replace('_', ' ').title()}{STATUS_COLORS['reset']}"
            write_line(f"{name:<40} | {status_str}")

    def print_dag(self, status_map: dict[str, MigrationStatus]) -> None:
        migration_tree = self.migrations_tree
        first_migration = next(iter(migration_tree))
        ChangelogFile._print_dag_rec(first_migration, migration_tree, status_map)

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
