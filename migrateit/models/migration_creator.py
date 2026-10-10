"""Migration file creation and index management.

Handles creation of new migration SQL files, index collision detection,
and user confirmation via inquirer.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import inquirer

from migrateit import constants as C
from migrateit.models.migration import Migration, get_migration_header
from migrateit.reporters.logs import logger
from migrateit.reporters.output import write_line


class _ChangelogSurface(Protocol):
    """Minimal interface for changelog data needed by migration creation."""

    def exist_migration_by_name(self, name: str) -> bool: ...
    def get_migration_by_name(self, name: str) -> Migration: ...
    def save(self) -> None: ...

    @property
    def migrations(self) -> list[Migration]: ...


def create(
    changelog: _ChangelogSurface,
    migrations_dir: Path,
    name: str,
    dependencies: tuple[str, ...] | None = None,
) -> Migration:
    """Create a new migration file in the given directory.

    Args:
        changelog: Changelog surface providing lookup and persistence.
        migrations_dir: Path to the migrations directory.
        name: The name of the new migration (must be a valid identifier).
        dependencies: List of migration names that this migration depends on.

    Returns:
        A new Migration instance.

    Raises:
        ValueError: If the name is invalid or dependencies don't exist.
        FileExistsError: If the user declines to overwrite an existing file.
    """
    if not name.isidentifier():
        raise ValueError(f"Migration name '{name}' is not a valid identifier")

    if dependencies and not all(changelog.exist_migration_by_name(dep) for dep in dependencies):
        raise ValueError(f"Some dependencies {dependencies} do not exist in the changelog")
    if dependencies:
        resolved_deps = tuple(changelog.get_migration_by_name(dep).name for dep in dependencies)
    else:
        resolved_deps = None

    is_initial = len(changelog.migrations) == 0
    if is_initial and dependencies:
        raise ValueError("Initial migration cannot have dependencies")

    new_filepath = _get_next_migration_path(migrations_dir, changelog.migrations, name)

    new_filepath.write_text(get_migration_header(new_filepath) + C.ROLLBACK_SPLIT_TAG + "\n\n")
    logger.debug("Created migration file: %s", new_filepath.name)

    new_migration = Migration(
        name=new_filepath.name,
        initial=is_initial,
        parents=() if is_initial else (resolved_deps or (changelog.migrations[-1].name,)),
    )
    changelog.migrations.append(new_migration)
    changelog.save()
    write_line(f"\tMigration {new_migration.name} created successfully")
    if resolved_deps:
        write_line(f"\tAdded dependencies to: {', '.join(resolved_deps)}")
    return new_migration


def _get_next_migration_path(
    migrations_dir: Path,
    migrations: list[Migration],
    name: str,
) -> Path:
    """Determine the next migration file path, handling index collisions."""
    migration_index = f"{len(migrations):04d}"
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
