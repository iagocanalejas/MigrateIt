import re
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


def create_new_migration(
    changelog: ChangelogFile,
    migrations_dir: Path,
    name: str,
    dependencies: list[str] | None = None,
) -> Migration:
    """
    Create a new migration file in the given directory.
    Args:
        changelog: The changelog file to update.
        migrations_dir: Path to the migrations directory.
        name: The name of the new migration (must be a valid identifier).
        dependencies: List of migration names that this migration depends on.
    Returns:
        A new Migration instance.
    """
    if not name.isidentifier():
        raise ValueError(f"Migration name '{name}' is not a valid identifier")

    migration_files = [m.name for m in changelog.migrations]

    # check if the name already exists and retrieve the full name
    if dependencies and not all(changelog.exist_migration_by_name(dep) for dep in dependencies):
        raise ValueError(f"Some dependencies {dependencies} do not exist in the changelog")
    dependencies = [changelog.get_migration_by_name(dep).name for dep in dependencies] if dependencies else None

    # check if this is the initial migration
    is_initial = len(migration_files) == 0
    if is_initial and dependencies:
        raise ValueError("Initial migration cannot have dependencies")

    # check if the name already exists (only can happen if a file was manually created)
    new_filepath = migrations_dir / f"{len(migration_files):04d}_{name}.sql"
    if new_filepath.exists():
        raise FileExistsError(f"Migration file {new_filepath.name} already exists")

    # create the new migration file with a header and rollback tag
    content = f"-- Migration {new_filepath.name}\n-- Created on {datetime.now().isoformat()}\n\n\n{ROLLBACK_SPLIT_TAG}"
    new_filepath.write_text(content)

    new_migration = Migration(
        name=new_filepath.name,
        initial=is_initial,
        parents=[] if is_initial else (dependencies or [migration_files[-1]]),
    )
    changelog.migrations.append(new_migration)
    save_changelog_file(changelog)
    write_line(f"\tMigration {new_migration.name} created successfully")
    if dependencies:
        write_line(f"\tAdded dependencies to: {', '.join(dependencies)}")
    return new_migration


def write_into_migration_file(migration_file: Path, sql: str | None, rollback: str | None) -> None:
    """
    Write SQL and rollback SQL into a migration file.
    Args:
        migration_file: The path to the migration file.
        sql: The SQL to write into the migration file.
        rollback: The rollback SQL to write into the migration file.
    """
    if (sql is None or not sql.strip()) and (rollback is None or not rollback.strip()):
        raise ValueError("At least one of sql or rollback must be provided")

    migration_content = migration_file.read_text(encoding="utf-8")
    if ROLLBACK_SPLIT_TAG not in migration_content:
        raise ValueError(f"{migration_file=} does not contain a rollback section ({ROLLBACK_SPLIT_TAG})")

    parts = migration_content.split(ROLLBACK_SPLIT_TAG, maxsplit=1)
    new_content = (
        parts[0].rstrip()
        + "\n\n"
        + (sql.strip() if sql else "")
        + "\n\n"
        + ROLLBACK_SPLIT_TAG
        + parts[1].rstrip()
        + "\n\n"
        + (rollback.strip() if rollback else "")
    )
    new_content = re.sub(r"\n{3,}", "\n\n", new_content)

    migration_file.write_text(new_content, encoding="utf-8")


def retrieve_migration_sqls(migration_file: Path) -> tuple[str | None, str | None]:
    """
    Retrieve the SQL and rollback SQL from a migration file.
    Args:
        migration_file: The path to the migration file.
    Returns:
        A tuple of (SQL, rollback SQL).
    """

    def remove_description_comments(content: str) -> str:
        return "\n".join(
            [
                w
                for w in content.splitlines()
                if not w.strip().startswith("-- Migration") and not w.strip().startswith("-- Created on")
            ]
        )

    if not migration_file.is_file() or not migration_file.name.endswith(".sql"):
        raise ValueError(f"Migration {migration_file.name} is not a valid SQL file")

    content = migration_file.read_text(encoding="utf-8")
    if ROLLBACK_SPLIT_TAG not in content:
        return remove_description_comments(content).strip(), None

    sql, rollback_sql = content.split(ROLLBACK_SPLIT_TAG, maxsplit=1)
    return remove_description_comments(sql).strip(), rollback_sql.strip()


def create_changelog_file(migrations_file: Path, database: SupportedDatabase) -> ChangelogFile:
    """
    Create a new changelog file with the initial version.
    Args:
        migrations_file: The path to the migrations file.
        database: The database type.
    """
    if migrations_file.exists():
        raise ValueError(f"File {migrations_file.name} already exists")
    if not migrations_file.name.endswith(".json"):
        raise ValueError(f"File {migrations_file.name} must be a JSON file")
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
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path.name} does not exist")
    changelog = ChangelogFile.from_json(file_path.read_text(), file_path)
    if not changelog.migrations:
        return changelog

    # Check if the migrations are valid
    if len([m for m in changelog.migrations if m.initial]) > 1:
        raise ValueError("Changelog must have exactly one initial migration")
    for m in changelog.migrations:
        if m.initial and len(m.parents) > 0:
            raise ValueError(f"Initial migration {m.name} cannot have parents")
        if not m.initial and len(m.parents) == 0:
            raise ValueError(f"Migration {m.name} must have parents")

    return changelog


def save_changelog_file(changelog: ChangelogFile) -> None:
    """
    Save the changelog file to the specified path.
    Args:
        changelog: The changelog file to save.
    """
    if not changelog.path.exists():
        raise FileNotFoundError(f"File {changelog.path.name} does not exist")
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
    """
    Build a migration plan based on the changelog and migration tree.
    Args:
        changelog: The changelog file containing migrations.
        migration_tree: An ordered dictionary representing the migration tree.
        statuses_map: A map of migration names to their statuses.
        target_migration: The target migration to apply or rollback to.
        is_rollback: Whether the plan is for a rollback operation.
    Returns:
        A list of migrations to apply or rollback, in the correct order.
    """
    plan: list[Migration] = []
    visited: set[str] = set()
    queue: deque[Migration] = deque([changelog.migrations[0]])
    is_bottom_up = target_migration is not None and not is_rollback

    if is_rollback:
        if not target_migration:
            raise ValueError("Target migration is required for rollback plan")
        queue = deque([target_migration])

    def get_neighbors(m: Migration) -> list[str]:
        # get the children of the migration
        return [m.name for m in migration_tree.get(m.name, [])]

    if is_bottom_up:
        if not target_migration:
            raise ValueError("Target migration is required for bottom-up plan")
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


def find_path(tree: OrderedDict[str, list[Migration]], parent: str, child: str, path: list[str] = []) -> list[str]:
    """
    Find a path from parent to child in the migration tree.
    Args:
        tree: The migration tree as an OrderedDict.
        parent: The starting migration name.
        child: The target migration name.
    Returns:
        A list of migration names representing the path from parent to child, or an empty list if no path exists.
    """
    path.append(parent)
    if parent == child:
        return path
    for next_child in tree.get(parent, []):
        result = find_path(tree, next_child.name, child, path)
        if result:
            return result
    path.pop()
    return []
