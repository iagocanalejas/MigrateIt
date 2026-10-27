from typing import Any

from migrateit.clients._client import SqlClient
from migrateit.models.migration import (
    MigrationStatus,
    retrieve_migration_sqls,
    write_into_migration_file,
)
from migrateit.models.repository import ChangelogRepository
from migrateit.reporters import write_line
from migrateit.reporters.logs import logger
from migrateit.reporters.output import write_line_error


def cmd_squash(
    client: SqlClient[Any],
    start_migration_name: str,
    end_migration_name: str | None = None,
    name: str | None = None,
) -> int:
    if not client.is_migrations_table_created():
        write_line_error(f"Migrations table '{client.table_name}' does not exist. Please run `init` & `migrate` first.")
        return 1

    if not end_migration_name:
        if len(client.changelog.migrations) < 3:
            write_line_error("Cannot squash less than 3 migrations.")
            return 1
        end_migration_name = client.changelog.migrations[-1].name

    start_migration = client.migrations.get_migration_by_name(start_migration_name)
    end_migration = client.migrations.get_migration_by_name(end_migration_name)
    write_line(f"Squashing migrations from '{start_migration.name}' to '{end_migration.name}'.")

    if not name or not name.strip():
        name = f"squashed_{start_migration.index}_{end_migration.index}"

    to_squash = client.migrations.find_path(start_migration.name, end_migration.name)
    if not to_squash:
        write_line_error(f"No path found from '{start_migration.name}' to '{end_migration.name}'.")
        return 1
    if any(m.initial for m in (client.migrations.get_migration_by_name(m) for m in to_squash)):
        write_line_error("Cannot squash initial migration.")
        return 1
    write_line(f"Following migrations will be squashed: {', '.join(to_squash)}.")

    statuses = client.retrieve_migration_statuses()
    first_status = statuses.get(to_squash[0])
    if not all(statuses.get(m) == first_status for m in to_squash):
        write_line_error("Cannot squash migrations that are not in the same state.")
        return 1

    squashed_migration = client.migrations.create_new_migration(
        name=name,
        dependencies=start_migration.parents,
        isolated=True,
    )

    for migration_name in to_squash:
        migration = client.migrations.get_migration_by_name(migration_name)
        logger.debug("Merging %s into %s", migration.name, squashed_migration.name)
        write_line(f"Squashing migration: '{migration.name}'.")
        sql, rollback = retrieve_migration_sqls(migration.path)
        updated = write_into_migration_file(squashed_migration.path, sql=sql, rollback=rollback)
        if not updated:
            write_line_error(f"No SQL content to write into the migration file for '{migration.name}'.")

        new_migration = migration.rename_squashed()
        write_line(f"Renaming migration '{migration.name}' to '{new_migration.name}'.")

    write_line(f"Squashed migration created: '{squashed_migration.name}'.")

    are_migrations_applied = statuses[to_squash[0]] == MigrationStatus.APPLIED
    if are_migrations_applied:
        # if all the migrations are already applied we need to update the database
        client.squash_migrations(to_squash, squashed_migration)
        client.connection.commit()
        write_line("Migrations marked as squashed in the database.")
        write_line(f"Squashed migration '{squashed_migration.name}' applied in the database.")

    write_line("Updating migration dependencies in the changelog.")
    client.migrations.squash_migrations(
        squashed_migration,
        to_squash,
        into=client.changelog.migrations.index(start_migration),
    )

    ChangelogRepository.save(client.changelog)
    write_line(f"Changelog updated: removed {len(to_squash)} migration(s), added '{squashed_migration.name}'.")

    return 0
