import functools
import json
import os
import re
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import inquirer

from migrateit import constants as C
from migrateit.models.migration import Migration, MigrationStatus, get_migration_header
from migrateit.reporters.logs import logger
from migrateit.reporters.output import STATUS_COLORS, write_line


class SupportedDatabase(Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    MYSQL = "mysql"
    MARIADB = "mariadb"


@dataclass
class ChangelogFile:
    version: int
    database: SupportedDatabase = SupportedDatabase.POSTGRES
    migrations: list[Migration] = field(default_factory=list)
    path: Path = field(default_factory=Path)

    @functools.cached_property
    def root(self) -> Migration:
        if len(self.migrations) == 0:
            raise ValueError("No migrations found. Changelog is not initialized.")
        if not self.migrations[0].initial:
            raise ValueError("Initial migration is not defined in the changelog")
        return self.migrations[0]

    @functools.cached_property
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

    def save(self) -> None:
        """
        Save the changelog file to the specified path.
        Args:
            changelog: The changelog file to save.
        """
        if not self.path.exists():
            raise FileNotFoundError(f"File {self.path.name} does not exist")
        self.path.write_text(self.to_json())
        logger.debug("Saved changelog: %s (%d migration(s))", self.path, len(self.migrations))
        write_line(f"\tMigrations file updated: {self.path}")

    def exist_migration_by_name(self, name: str) -> bool:
        name = os.path.basename(name) if os.path.isabs(name) else name
        prefix = name.split("_", 1)[0]
        return any(m.name.startswith(prefix) for m in self.migrations)

    def get_migration_by_name(self, name: str) -> Migration:
        if os.path.isabs(name):
            name = os.path.basename(name)
        index = name.split("_")[0]  # get the migration number
        if not re.match(r"^[0-9]{4}$", index):
            raise ValueError(f"Invalid {index=} for migration='{name}'")
        for migration in self.migrations:
            if migration.name.split("_")[0] == index:
                return migration
        raise ValueError(f"Migration '{name}' not found in changelog")

    def create_new_migration(
        self,
        migrations_dir: Path,
        name: str,
        dependencies: tuple[str, ...] | None = None,
    ) -> Migration:
        """
        Create a new migration file in the given directory.
        Args:
            migrations_dir: Path to the migrations directory.
            name: The name of the new migration (must be a valid identifier).
            dependencies: List of migration names that this migration depends on.
        Returns:
            A new Migration instance.
        """
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
        self.save()
        write_line(f"\tMigration {new_migration.name} created successfully")
        if dependencies:
            write_line(f"\tAdded dependencies to: {', '.join(dependencies)}")
        return new_migration

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
        is_bottom_up = target_migration is not None and not is_rollback
        is_normal_order = not is_bottom_up and not is_rollback

        queue: deque[Migration] = deque([self.root])
        in_queue = {self.root.name}  # NOTE: optimization to avoid O(n) membership check on deque
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
                if neighbor.name not in visited and neighbor.name not in in_queue:
                    # Structure: A → B, A → C, B → C
                    # Tree: { A: [B, C], B: [C], C: [] }
                    # when: is_bottom_up=True
                    # C is a common child of A and B. It gets added to the queue twice, so we skip the second visit.
                    in_queue.add(neighbor.name)
                    queue.append(neighbor)
            in_queue.remove(current.name)

        plan = list(reversed(plan)) if not is_normal_order else plan
        if is_rollback:
            return [p for p in plan if statuses_map[p.name] == MigrationStatus.APPLIED]
        return [p for p in plan if statuses_map[p.name] != MigrationStatus.APPLIED]

    def find_path(self, parent: str, child: str, path: list[str] | None = None) -> list[str]:
        """
        Find a path from parent to child in the migration tree.
        Args:
            parent: The starting migration name.
            child: The target migration name.
        Returns:
            A list of migration names representing the path from parent to child, or an empty list if no path exists.
        """
        path = [] if path is None else list(path)
        path.append(parent)
        if parent == child:
            return path
        for next_child in self.migrations_tree.get(parent, []):
            result = self.find_path(next_child.name, child, path)
            if result:
                return result
        path.pop()
        return []

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
        seen: set[str] | None = None,
    ) -> None:
        seen = seen or set()
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


def create_changelog_file(migrations_file: Path, database: SupportedDatabase) -> ChangelogFile:
    """
    Create a new changelog file with the initial version.
    Args:
        migrations_file: The path to the migrations file.
        database: The database type.
    """
    if migrations_file.exists():
        raise FileExistsError(f"File {migrations_file.name} already exists")
    if not migrations_file.name.endswith(".json"):
        raise ValueError(f"File {migrations_file.name} must be a JSON file")
    changelog = ChangelogFile(version=1, database=database)
    migrations_file.parent.mkdir(parents=True, exist_ok=True)
    migrations_file.write_text(changelog.to_json())
    return load_changelog_file(migrations_file)


def load_changelog_file(file_path: Path) -> ChangelogFile:
    """
    Load a changelog file from the specified path.
    Args:
        file_path: The path to the migrations file.
    Returns:
        ChangelogFile: The loaded migrations file.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path.name} does not exist")
    changelog = ChangelogFile.from_json(file_path.read_text(), file_path)
    if not changelog.migrations:
        return changelog

    # Check if the migrations are valid
    if sum(1 for m in changelog.migrations if m.initial) != 1:
        raise ValueError("Changelog must have exactly one initial migration")

    seen_names = set()
    seen_indexes = set()
    for m in changelog.migrations:
        seen_names.add(m.name)
        if m.initial and len(m.parents) > 0:
            raise ValueError(f"Initial migration {m.name} cannot have parents")
        if not m.initial and len(m.parents) == 0:
            raise ValueError(f"Migration {m.name} must have parents")
        if not re.match(C.MIGRATION_PATTERN, m.name):
            raise ValueError(f"Migration name {m.name} does not match the pattern")

        # Check if the migration index is unique
        index = int(m.name.split("_")[0])
        if index in seen_indexes:
            raise ValueError(f"Migration name {m.name} has a duplicated index")
        seen_indexes.add(index)

        if not all(p in seen_names for p in m.parents):
            # NOTE: you can't have a parent saved after you in the changelog
            raise ValueError(f"Migration {m.name} references a non-existent parent")

    return changelog
