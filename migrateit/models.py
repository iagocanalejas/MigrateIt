import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path


@dataclass
class MigrateItConfig:
    table_name: str
    migrations_dir: Path
    migrations_file: Path


class MigrationStatus(Enum):
    APPLIED = "applied"
    CONFLICT = "conflict"
    REMOVED = "removed"
    NOT_APPLIED = "not_applied"


@dataclass
class Migration:
    name: str

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
        return {"name": self.name}

    @classmethod
    def create_new(cls, migrations_dir: Path, migrations_file: Path, name: str) -> "Migration":
        """
        Create a new migration file in the given directory.
        Args:
            migrations_dir: Path to the migrations directory.
            migrations_file: Path to the file storing the migration metadata.
            name: The name of the new migration (must be a valid identifier).
        Returns:
            A new Migration instance.
        """
        assert name, "Migration name cannot be empty"
        assert name.isidentifier(), f"Migration {name=} is not a valid identifier"

        migration_files = [f for f in migrations_dir.iterdir() if cls.is_valid_name(f)]

        new_filepath = migrations_dir / f"{len(migration_files):04d}_{name}.sql"
        assert not new_filepath.exists(), f"File {new_filepath.name} already exists"

        new_filepath.write_text(
            f"-- Migration {new_filepath.name}\n-- Created on {datetime.now().isoformat()}",
        )

        migrations = MigrationsFile.load_file(migrations_file)
        migrations.migrations.append(cls(name=new_filepath.name))
        migrations.save_file(migrations_file)
        print("\tNew migration file created:", new_filepath.name)
        return migrations.migrations[-1]


@dataclass
class MigrationsFile:
    version: int
    migrations: list[Migration] = field(default_factory=list)

    @staticmethod
    def from_json(json_str: str) -> "MigrationsFile":
        data = json.loads(json_str)
        try:
            migrations = [Migration(**m) for m in data.get("migrations", [])]
            return MigrationsFile(
                version=data["version"],
                migrations=migrations,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"Invalid JSON for MigrationsFile: {e}")

    @staticmethod
    def create_file(migrations_file: Path) -> None:
        """
        Create a new migrations file with the initial version.
        Args:
            migrations_file: The path to the migrations file.
        """
        assert not migrations_file.exists(), f"File {migrations_file.name} already exists"
        assert migrations_file.name.endswith(".json"), f"File {migrations_file.name} must be a JSON file"
        migrations_file.write_text(MigrationsFile(version=1).to_json())

    @staticmethod
    def load_file(file_path: Path) -> "MigrationsFile":
        """
        Load a migrations file from the specified path.
        Args:
            file_path: The path to the migrations file.
        Returns:
            MigrationsFile: The loaded migrations file.
        """
        assert file_path.exists(), f"File {file_path.name} does not exist"
        return MigrationsFile.from_json(file_path.read_text())

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "migrations": [migration.to_dict() for migration in self.migrations],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=4)

    def save_file(self, file_path: Path) -> None:
        """
        Save the migrations file to the specified path.
        Args:
            file_path: The path to save the migrations file.
        """
        assert file_path.exists(), f"File {file_path.name} does not exist"
        file_path.write_text(self.to_json())
        print("\tMigrations file updated:", file_path)
