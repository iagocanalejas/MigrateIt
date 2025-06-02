from collections import OrderedDict, deque
from datetime import datetime
from pathlib import Path

from migrateit.models import ChangelogFile, Migration
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.migration import MigrationStatus
from migrateit.reporters import write_line


def create_migration_directory(migrations_dir: Path) -> None:
    """
    Create the migrations directory if it doesn't exist.
    Args:
        migrations_dir: The path to the migrations directory.
    """
    migrations_dir.mkdir(parents=True, exist_ok=True)


ROLLBACK_SPLIT_TAG = "-- Rollback migration"


def create_new_migration(changelog: ChangelogFile, migrations_dir: Path, name: str) -> Migration:
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
    content = f"-- Migration {new_filepath.name}\n-- Created on {datetime.now().isoformat()}\n\n\n{ROLLBACK_SPLIT_TAG}"
    new_filepath.write_text(content)

    is_initial = len(migration_files) == 0
    new_migration = Migration(
        name=new_filepath.name,
        initial=is_initial,
        parents=[] if is_initial else [migration_files[-1]],
    )
    changelog.migrations.append(new_migration)
    save_changelog_file(changelog)
    write_line(f"\tMigration {new_migration.name} created successfully")
    return new_migration


def create_changelog_file(migrations_file: Path, database: SupportedDatabase) -> ChangelogFile:
    """
    Create a new changelog file with the initial version.
    Args:
        migrations_file: The path to the migrations file.
        database: The database type.
    """
    assert not migrations_file.exists(), f"File {migrations_file.name} already exists"
    assert migrations_file.name.endswith(".json"), f"File {migrations_file.name} must be a JSON file"
    changelog = ChangelogFile(version=1, database=database)
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


def save_changelog_file(changelog: ChangelogFile) -> None:
    """
    Save the changelog file to the specified path.
    Args:
        changelog: The changelog file to save.
    """
    assert changelog.path.exists(), f"File {changelog.path.name} does not exist"
    changelog.path.write_text(changelog.to_json())
    write_line(f"\tMigrations file updated: {changelog.path}")


def build_migrations_tree(changelog: ChangelogFile) -> OrderedDict[str, list[Migration]]:
    """
    Build a tree of migrations and their childrens.
    """
    d = OrderedDict()
    for migration in changelog.migrations:
        if migration.name not in d:
            d[migration.name] = []
        for parent in migration.parents:
            d[parent].append(migration)
    return d


def build_migration_plan(
    changelog: ChangelogFile,
    migration_tree: OrderedDict[str, list[Migration]],
    statuses_map: dict[str, MigrationStatus],
    target_migration: Migration | None = None,
    is_rollback: bool = False,
) -> list[Migration]:
    plan: list[Migration] = []
    visited: set[str] = set()
    queue: deque[Migration] = deque([changelog.migrations[0]])
    is_bottom_up = target_migration is not None and not is_rollback

    if is_rollback:
        assert target_migration, "Target migration is required for rollback plan"
        queue = deque([target_migration])

    def get_neighbors(m: Migration) -> list[str]:
        # get the children of the migration
        return [m.name for m in migration_tree.get(m.name, [])]

    if is_bottom_up:
        assert target_migration, "Target migration is required for bottom-up plan"
        queue = deque([target_migration])

        def get_neighbors(m: Migration) -> list[str]:
            return list(reversed(m.parents))

    while queue:
        current = queue.popleft()
        if current.name in visited:
            continue

        if not is_bottom_up and not is_rollback and not all(p in visited for p in current.parents):
            queue.append(current)  # requeue
            continue

        visited.add(current.name)
        plan.append(current)
        for neighbor_name in get_neighbors(current):
            neighbor = changelog.get_migration_by_name(neighbor_name)
            if neighbor.name in visited:
                continue
            queue.append(neighbor)

    plan = list(reversed(plan)) if is_bottom_up or is_rollback else plan
    if is_rollback:
        return [p for p in plan if statuses_map[p.name] == MigrationStatus.APPLIED]
    return [p for p in plan if statuses_map[p.name] != MigrationStatus.APPLIED]
