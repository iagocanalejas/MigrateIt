import json
import os
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path


class SupportedDatabase(Enum):
    POSTGRES = "postgres"


class MigrationStatus(Enum):
    APPLIED = "applied"
    CONFLICT = "conflict"
    REMOVED = "removed"
    NOT_APPLIED = "not_applied"


@dataclass
class MigrateItConfig:
    table_name: str
    migrations_dir: Path
    changelog: "ChangelogFile"


@dataclass
class Migration:
    name: str
    initial: bool = False
    parents: list[str] = field(default_factory=list)

    @staticmethod
    def is_valid_name(migration: Path) -> bool:
        return (
            migration.is_file() and migration.name.endswith(".sql") and re.match(r"^\d{4}_", migration.name) is not None
        )

    @staticmethod
    def create_directory(migrations_dir: Path) -> None:
        """
        Create the migrations directory if it doesn't exist.
        Args:
            migrations_dir: The path to the migrations directory.
        """
        migrations_dir.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "initial": self.initial,
            "parents": self.parents,
        }

    @classmethod
    def create_new(
        cls,
        changelog: "ChangelogFile",
        migrations_dir: Path,
        name: str,
    ) -> "Migration":
        """
        Create a new migration file in the given directory.
        Args:
            changelog: The changelog file to update.
            migrations_dir: Path to the migrations directory.
            name: The name of the new migration (must be a valid identifier).
        Returns:
            A new Migration instance.
        """
        assert name, "Migration name cannot be empty"
        assert name.isidentifier(), f"Migration {name=} is not a valid identifier"

        migration_files = [m.name for m in changelog.migrations]

        new_filepath = migrations_dir / f"{len(migration_files):04d}_{name}.sql"
        assert not new_filepath.exists(), f"File {new_filepath.name} already exists"
        new_filepath.write_text(f"-- Migration {new_filepath.name}\n-- Created on {datetime.now().isoformat()}")

        is_initial = len(migration_files) == 0
        new_migration = cls(
            name=new_filepath.name,
            initial=is_initial,
            parents=[] if is_initial else [migration_files[-1]],
        )
        changelog.migrations.append(new_migration)
        changelog.save_file()
        print("\tNew migration file created:", new_filepath.name)
        return new_migration


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

    @staticmethod
    def create_file(migrations_file: Path, database: SupportedDatabase) -> "ChangelogFile":
        """
        Create a new migrations file with the initial version.
        Args:
            migrations_file: The path to the migrations file.
            database: The database type.
        """
        assert not migrations_file.exists(), f"File {migrations_file.name} already exists"
        assert migrations_file.name.endswith(".json"), f"File {migrations_file.name} must be a JSON file"
        changelog = ChangelogFile(version=1, database=database)
        migrations_file.write_text(changelog.to_json())
        return changelog

    @staticmethod
    def load_file(file_path: Path) -> "ChangelogFile":
        """
        Load a migrations file from the specified path.
        Args:
            file_path: The path to the migrations file.
        Returns:
            ChangelogFile: The loaded migrations file.
        """
        assert file_path.exists(), f"File {file_path.name} does not exist"
        changelog = ChangelogFile.from_json(file_path.read_text(), file_path)
        if not changelog.migrations:
            return changelog

        # Check if the migrations are valid
        assert len([m for m in changelog.migrations if m.initial]) <= 1, "Only one initial migration is allowed"
        for m in changelog.migrations:
            assert not m.initial or len(m.parents) == 0, f"Initial migration {m.name} cannot have parents"
            assert m.initial or len(m.parents) > 0, f"Migration {m.name} must have parents"

        return changelog

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "database": self.database.value,
            "migrations": [migration.to_dict() for migration in self.migrations],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=4)

    def save_file(self) -> None:
        """Save the migrations file to the specified path."""
        assert self.path.exists(), f"File {self.path.name} does not exist"
        self.path.write_text(self.to_json())
        print("\tMigrations file updated:", self.path)

    def get_migration_by_name(self, name: str) -> Migration | None:
        if os.path.isabs(name):
            name = os.path.basename(name)
        name = name.split("_")[0]  # get the migration number
        for migration in self.migrations:
            if migration.name.startswith(name):
                return migration

    def build_migration_plan(self, migration: Migration | None = None) -> list[Migration]:
        if migration:
            return self._build_bottom_up_migration_plan(migration)
        return self._build_top_down_migration_plan()

    def _build_bottom_up_migration_plan(self, migration: Migration) -> list[Migration]:
        plan: list[Migration] = []
        visited: set[str] = set()
        queue: deque[Migration] = deque([migration])

        while queue:
            current = queue.popleft()
            if current.name in visited:
                continue
            visited.add(current.name)
            plan.append(current)

            for parent_name in current.parents:
                parent = self.get_migration_by_name(parent_name)
                if parent and parent.name not in visited:
                    queue.append(parent)

        return list(reversed(plan))

    def _build_top_down_migration_plan(self) -> list[Migration]:
        root, tree = self.graph
        root = self.get_migration_by_name(root)
        assert root, f"Migration {root} not found in changelog"

        plan: list[Migration] = []
        visited: set[str] = set()
        queue: deque[Migration] = deque([root])

        while queue:
            current = queue.popleft()
            if current.name in visited:
                continue

            if not all(p in visited for p in current.parents):
                queue.append(current)  # requeue
                continue

            visited.add(current.name)
            plan.append(current)
            for child_name in tree.get(current.name, []):
                child = self.get_migration_by_name(child_name)
                if child and child.name not in visited:
                    queue.append(child)
        return plan
