from typing import Any

from migrateit.clients._client import SqlClient
from migrateit.models.migration import (
    MigrationStatus,
)
from migrateit.models.repository import ChangelogRepository
from migrateit.reporters import write_line
from migrateit.reporters.output import write_line_error


def cmd_drop(client: SqlClient[Any], name: str) -> int:
    target_migration = client.migrations.get_migration_by_name(name)
    if any(target_migration.name in m.parents for m in client.changelog.migrations):
        write_line_error(f"Cannot drop migration '{name}': it is a parent of another migration.")
        return 1
    if target_migration.initial:
        write_line_error("Cannot drop the initial migration.")
        return 1

    write_line(f"Dropping migration '{target_migration.name}'.")
    statuses = client.retrieve_migration_statuses()
    if statuses.get(target_migration.name) == MigrationStatus.APPLIED:
        client.apply_migration(target_migration, is_rollback=True)
        write_line("\tMigration rolled back from the database.")

    path = target_migration.path  # NOTE: save it as calling target_migration.path will fail after we unlink it
    path.unlink()
    write_line(f"\tMigration removed from {path.name}.")

    client.changelog.migrations.remove(target_migration)
    ChangelogRepository.save(client.changelog)
    write_line(f"Migration '{target_migration.name}' dropped and removed from changelog.")
    return 0
