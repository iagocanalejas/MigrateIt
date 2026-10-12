from __future__ import annotations

import os
from collections import deque
from pathlib import Path

import inquirer

from migrateit import constants as C
from migrateit.models.changelog import Changelog
from migrateit.models.migration import Migration, MigrationStatus, get_migration_header
from migrateit.reporters import STATUS_COLORS
from migrateit.reporters.logs import logger
from migrateit.reporters.output import write_line


class MigrationManager:
    def __init__(self, changelog: Changelog) -> None:
        self._changelog = changelog

    @property
    def migrations(self) -> list[Migration]:
        return self._changelog.migrations

    def create_new_migration(
        self,
        migrations_dir: Path,
        name: str,
        dependencies: tuple[str, ...] | None = None,
    ) -> Migration:
        if not name.isidentifier():
            raise ValueError(f"Migration name '{name}' is not a valid identifier")

        if dependencies and not all(self.exist_migration_by_name(dep) for dep in dependencies):
            raise ValueError(f"Some dependencies {dependencies} do not exist in the changelog")
        dependencies = tuple(self.get_migration_by_name(dep).name for dep in dependencies) if dependencies else None

        is_initial = len(self.migrations) == 0
        if is_initial and dependencies:
            raise ValueError("Initial migration cannot have dependencies")

        new_filepath = self._get_next_migration_path(migrations_dir, name)

        # create the new migration file with a header and rollback tag
        new_filepath.write_text(get_migration_header(new_filepath) + C.ROLLBACK_SPLIT_TAG + "\n\n")
        logger.debug("Created migration file: %s", new_filepath.name)

        new_migration = Migration(
            name=new_filepath.name,
            initial=is_initial,
            parents=() if is_initial else (dependencies or (self.migrations[-1].name,)),
        )
        self.migrations.append(new_migration)
        write_line(f"\tMigration {new_migration.name} created successfully")
        if dependencies:
            write_line(f"\tAdded dependencies to: {', '.join(dependencies)}")
        return new_migration

    def get_migration_by_name(self, name: str) -> Migration:
        if os.path.isabs(name):
            name = os.path.basename(name)
        index = name.split("_")[0]
        if not index.isdigit() or len(index) != 4:
            raise ValueError(f"Invalid {index=} for migration='{name}'")
        for migration in self.migrations:
            if migration.name.split("_")[0] == index:
                return migration
        raise ValueError(f"Migration '{name}' not found in changelog")

    def exist_migration_by_name(self, name: str) -> bool:
        name = os.path.basename(name) if os.path.isabs(name) else name
        prefix = name.split("_", 1)[0]
        return any(m.name.startswith(prefix) for m in self.migrations)

    def build_migration_plan(
        self,
        statuses_map: dict[str, MigrationStatus],
        target_migration: Migration | None = None,
        is_rollback: bool = False,
    ) -> list[Migration]:
        plan: list[Migration] = []
        visited: set[str] = set()
        is_bottom_up = target_migration is not None and not is_rollback
        is_normal_order = not is_bottom_up and not is_rollback

        queue: deque[Migration] = deque([self._changelog.root])
        in_queue = {self._changelog.root.name}
        if is_rollback:
            if not target_migration:
                raise ValueError("Target migration is required for rollback plan")
            queue = deque([target_migration])
            in_queue = {target_migration.name}

        if is_bottom_up:
            if not target_migration:
                raise ValueError("Target migration is required for bottom-up plan")
            queue = deque([target_migration])
            in_queue = {target_migration.name}

        def get_neighbors(m: Migration) -> list[str]:
            return (
                list(reversed(m.parents))
                if is_bottom_up
                else [n.name for n in self._changelog.migrations_tree.get(m.name, [])]
            )

        while queue:
            current = queue.popleft()

            has_unvisited_parents = any(p not in visited for p in current.parents)
            if is_normal_order and has_unvisited_parents:
                queue.append(current)
                continue

            visited.add(current.name)
            plan.append(current)
            for neighbor_name in get_neighbors(current):
                neighbor = self.get_migration_by_name(neighbor_name)
                if neighbor.name not in visited and neighbor.name not in in_queue:
                    in_queue.add(neighbor.name)
                    queue.append(neighbor)
            in_queue.remove(current.name)

        plan = list(reversed(plan)) if not is_normal_order else plan
        if is_rollback:
            return [p for p in plan if statuses_map[p.name] == MigrationStatus.APPLIED]
        return [p for p in plan if statuses_map[p.name] != MigrationStatus.APPLIED]

    def find_path(self, parent: str, child: str, path: list[str] | None = None) -> list[str]:
        path = [] if path is None else list(path)
        path.append(parent)
        if parent == child:
            return path
        for next_child in self._changelog.migrations_tree.get(parent, []):
            result = self.find_path(next_child.name, child, path)
            if result:
                return result
        path.pop()
        return []

    def print_list(self, status_map: dict[str, MigrationStatus]) -> None:
        migration_tree = self._changelog.migrations_tree
        for name in migration_tree.keys():
            status = status_map[name]
            status_str = f"{STATUS_COLORS[status]}{status.name.replace('_', ' ').title()}{STATUS_COLORS['reset']}"
            write_line(f"{name:<40} | {status_str}")

    def print_dag(self, status_map: dict[str, MigrationStatus]) -> None:
        migration_tree = self._changelog.migrations_tree
        first_migration = next(iter(migration_tree))
        MigrationManager._print_dag_rec(first_migration, migration_tree, status_map)

    @staticmethod
    def _print_dag_rec(
        name: str,
        children: dict[str, list[Migration]],
        status_map: dict[str, MigrationStatus],
        level: int = 0,
        seen: set[str] | None = None,
    ) -> None:
        seen = seen or set()
        indent = "  " * level + ("└─ " if level > 0 else "")
        status = status_map[name]
        status_str = f"{STATUS_COLORS[status]}{status.name.replace('_', ' ').title()}{STATUS_COLORS['reset']}"

        repeat_marker = " (*)" if name in seen else ""
        write_line(f"{indent}{name:<40} | {status_str}{repeat_marker}")

        if name in seen:
            return
        seen.add(name)

        for child in children.get(name, []):
            MigrationManager._print_dag_rec(child.name, children, status_map, level + 1, seen)

    def _get_next_migration_path(self, migrations_dir: Path, name: str) -> Path:
        migration_index = f"{len(self.migrations):04d}"
        new_filepath = migrations_dir / f"{migration_index}_{name}.sql"
        if new_filepath.exists():
            if not inquirer.confirm(f"Migration file {new_filepath.name} already exists. Overwrite?"):
                raise FileExistsError(f"Migration file {new_filepath.name} already exists")
            new_filepath.unlink()
        if len(list(migrations_dir.glob(f"{migration_index}_*.sql"))) > 0:
            if not inquirer.confirm(f"Migration index {migration_index} already exists. Overwrite?"):
                raise FileExistsError(f"Migration index {migration_index} already exists")
            for f in migrations_dir.glob(f"{migration_index}_*.sql"):
                write_line(f"Deleting {f.name}")
                f.unlink()
        return new_filepath
