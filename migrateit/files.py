import os
import re
from datetime import datetime

from migrateit.models import MigrateItConfig, Migration, MigrationsFile


def create_migrations_dir(folder_path: str):
    assert not os.path.exists(folder_path), f"Folder {folder_path} already exists"
    os.makedirs(folder_path, exist_ok=True)


def create_migrations_file(file_path: str):
    assert not os.path.exists(file_path), f"File {file_path} already exists"
    assert file_path.endswith(".json"), f"File {file_path} must be a JSON file"

    file = MigrationsFile(version=1)
    with open(file_path, "w") as f:
        f.write(file.to_json())


def load_migrations_file(file_path: str) -> MigrationsFile:
    assert os.path.exists(file_path), f"File {file_path} does not exist"
    with open(file_path, "r") as f:
        content = f.read()
        return MigrationsFile.from_json(content)


def save_migrations_file(file_path: str, migrations_file: MigrationsFile):
    assert os.path.exists(file_path), f"File {file_path} does not exist"
    with open(file_path, "w") as f:
        f.write(migrations_file.to_json())
    print("Migrations file updated:", file_path)


def create_new_migration(config: MigrateItConfig, name: str):
    def is_valid_migration_file(file_name: str) -> bool:
        return (
            os.path.isfile(os.path.join(config.migrations_dir, file_name))
            and file_name.endswith(".sql")
            and bool(re.match(r"^\d{4}_", file_name))
        )

    assert name, "Migration name cannot be empty"
    assert name.isidentifier(), f"Migration {name=} is not a valid identifier"

    migration_files = [f for f in os.listdir(config.migrations_dir) if is_valid_migration_file(f)]
    migrations = load_migrations_file(config.migrations_file)

    new_file = f"{len(migration_files):04d}_{name}.sql"
    assert not os.path.exists(os.path.join(config.migrations_dir, new_file)), f"File {new_file} already exists"

    with open(os.path.join(config.migrations_dir, new_file), "w") as f:
        f.write(
            f"""
-- Migration {new_file}
-- Created on {datetime.fromtimestamp(os.path.getctime(os.path.join(config.migrations_dir, new_file))).isoformat()}
""".strip()
        )

    print("New migration file created:", new_file)
    migrations.migrations.append(Migration(name=new_file))
    save_migrations_file(config.migrations_file, migrations)
