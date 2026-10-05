import os
import platform
import shlex
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

import inquirer

from migrateit.clients._client import SqlClient
from migrateit.constants import VALID_EDITORS
from migrateit.models.changelog import SupportedDatabase, create_changelog_file
from migrateit.models.migration import (
    MigrationStatus,
    create_migration_directory,
    retrieve_migration_sqls,
    write_into_migration_file,
)
from migrateit.reporters import STATUS_COLORS, pretty_print_sql_error, write_line
from migrateit.reporters.logs import logger


def _validate_editor(editor: str) -> str:
    basename = os.path.basename(editor)
    if basename not in VALID_EDITORS:
        if os.path.isabs(editor) and os.path.isfile(editor) and os.access(editor, os.X_OK):
            return editor
        raise ValueError(
            f"Unsafe editor: {editor!r}. "
            f"Known safe editors: {', '.join(sorted(VALID_EDITORS))}. "
            f"Or provide an absolute path to a valid executable."
        )
    return editor


def cmd_init(table_name: str, migrations_dir: Path, migrations_file: Path, database: SupportedDatabase) -> int:
    write_line(f"Initializing {database.value} database migrations")
    write_line(f"\tCreating migrations file: {migrations_file}")
    changelog = create_changelog_file(migrations_file, database)

    write_line(f"\tCreating migrations directory: {migrations_dir}")
    create_migration_directory(migrations_dir)

    write_line(f"\tCreating migration for table: {table_name}")
    migration = changelog.create_new_migration(migrations_dir=migrations_dir, name="migrateit")
    match database:
        case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            from migrateit.clients.mysql import MySqlClient

            sql, rollback = MySqlClient.create_migrations_table_str(table_name=table_name)
        case SupportedDatabase.POSTGRES:
            from migrateit.clients.psql import PsqlClient

            sql, rollback = PsqlClient.create_migrations_table_str(table_name=table_name)
        case SupportedDatabase.SQLITE:
            from migrateit.clients.sqlite import SqliteClient

            sql, rollback = SqliteClient.create_migrations_table_str(table_name=table_name)
        case _:
            raise NotImplementedError(f"Database {database} is not supported yet")

    write_into_migration_file(Path(migrations_dir / migration.name), sql=sql, rollback=rollback)
    write_line(f"Initialization complete: table={table_name} directory={migrations_dir}")

    return 0


def cmd_export(client: SqlClient[Any], name: str | None) -> int:
    if not name:
        name = f"{client.changelog.database.value}_backup"

    write_line(f"Creating new migration: {name}")
    migration = client.changelog.create_new_migration(
        migrations_dir=client.migrations_dir,
        name=name,
        dependencies=[client.changelog.root.name],
    )
    write_line(f"Migration file created: {migration.name}")

    client.export_database_schema(migration)
    write_line(f"Full database exported to: {migration.name}")
    return 0


def cmd_new(
    client: SqlClient[Any],
    name: str,
    dependencies: list[str] | None = None,
    no_edit: bool = False,
    interactive: bool = False,
) -> int:
    if not client.is_migrations_table_created():
        raise ValueError(f"Migrations table={client.table_name} does not exist. Please run `init` & `migrate` first.")
    if interactive and dependencies is not None and len(dependencies) > 0:
        raise ValueError("Cannot specify both `--interactive` and `--dependencies`")

    # NOTE: required in case-sensitive OSs
    name = name.lower()

    if interactive:
        choices = inquirer.prompt(
            [
                inquirer.Checkbox(
                    "dependencies",
                    message="Select dependencies for the migration",
                    choices=[m.name for m in client.changelog.migrations],
                )
            ],
        )
        dependencies = choices.get("dependencies", None) if choices else None

    write_line(f"Creating new migration: {name}")
    migration = client.changelog.create_new_migration(
        migrations_dir=client.migrations_dir,
        name=name,
        dependencies=dependencies,
    )
    write_line(f"Migration file created: {migration.name}")

    if no_edit:
        return 0

    editor = os.getenv("EDITOR", "notepad.exe" if platform.system() == "Windows" else "vim")
    editor = _validate_editor(editor)
    cmd = shlex.split(editor) + [str(client.migrations_dir / migration.name)]
    return subprocess.call(cmd)


def cmd_run(
    client: SqlClient[Any],
    name: str | None = None,
    is_fake: bool = False,
    is_rollback: bool = False,
    is_hash_update: bool = False,
) -> int:
    target_migration = client.changelog.get_migration_by_name(name) if name else None

    if is_hash_update:
        if not target_migration:
            raise ValueError("Hash update requires a target migration name")
        if target_migration.initial:
            raise ValueError("Cannot update hash for the initial migration")
        write_line(f"Updating hash for migration: {target_migration.name}")
        client.update_migration_hash(target_migration)
        client.connection.commit()
        write_line(f"Hash updated for {target_migration.name}")
        return 0

    statuses = client.retrieve_migration_statuses()
    if is_fake:
        if not target_migration:
            raise ValueError("Fake migration requires a target migration name")
        if target_migration.initial:
            raise ValueError("Cannot fake the initial migration")
        action = "Faking" if not is_rollback else "Faking rollback for"
        write_line(f"{action} migration: {target_migration.name}")
        client.apply_migration(target_migration, is_fake=is_fake, is_rollback=is_rollback)
        client.connection.commit()
        return 0

    if is_rollback and not target_migration:
        raise ValueError("Rollback requires a target migration name")
    client.validate_migrations(statuses)

    migration_plan = client.changelog.build_migration_plan(
        statuses_map=statuses,
        target_migration=target_migration,
        is_rollback=is_rollback,
    )

    if not migration_plan:
        if is_rollback:
            write_line("Rollback: no migrations to roll back")
        else:
            write_line("All migrations already applied")
        return 0

    action = "Applying" if not is_rollback else "Rolling back"
    if target_migration:
        write_line(f"Target: {target_migration.name}")
    write_line(f"{action} {len(migration_plan)} migration(s)")

    for migration in migration_plan:
        write_line(f"{action.lower().capitalize()} migration: {migration.name}")
        client.apply_migration(migration, is_rollback=is_rollback)

    client.connection.commit()
    direction = "Migration" if not is_rollback else "Rollback"
    write_line(f"{direction} complete: {len(migration_plan)} migration(s) applied")
    return 0


def cmd_squash(
    client: SqlClient[Any],
    start_migration: str,
    end_migration: str | None = None,
    name: str | None = None,
) -> int:
    if not client.is_migrations_table_created():
        raise ValueError(f"Migrations table={client.table_name} does not exist. Please run `init` & `migrate` first.")

    if not end_migration:
        if len(client.changelog.migrations) < 3:
            raise ValueError("Cannot squash less than 3 migrations.")
        end_migration = client.changelog.migrations[-1].name

    start_migration = client.changelog.get_migration_by_name(start_migration).name
    end_migration = client.changelog.get_migration_by_name(end_migration).name
    write_line(f"Squashing migrations from {start_migration} to {end_migration}.")

    to_squash = client.changelog.find_path(start_migration, end_migration)
    write_line(f"Following migrations will be squashed: {', '.join(to_squash)}")
    if not to_squash:
        raise ValueError(f"No path found from {start_migration} to {end_migration}.")
    if any(m.initial for m in (client.changelog.get_migration_by_name(m) for m in to_squash)):
        raise ValueError("Cannot squash initial migrations.")

    statuses = client.retrieve_migration_statuses()
    if not all(statuses[m] == statuses[to_squash[0]] for m in to_squash):
        raise ValueError("Cannot squash migrations that are not in the same state.")

    squashed_migration = client.changelog.create_new_migration(
        migrations_dir=client.migrations_dir,
        name=name if name else f"squashed_{start_migration}_{end_migration}",
        dependencies=client.changelog.get_migration_by_name(start_migration).parents,
    )

    for migration_name in to_squash:
        migration = client.changelog.get_migration_by_name(migration_name)
        logger.debug("Merging %s into %s", migration.name, squashed_migration.name)
        write_line(f"Squashing migration: {migration.name}")
        sql, rollback = retrieve_migration_sqls(client.migrations_dir / migration.name)
        write_into_migration_file(client.migrations_dir / squashed_migration.name, sql=sql, rollback=rollback)

    write_line(f"Squashed migration created: {squashed_migration.name}")

    are_migrations_applied = statuses[to_squash[0]] == MigrationStatus.APPLIED
    if are_migrations_applied:
        # if all the migrations are already applied we need to update the database
        client.squash_migrations(to_squash, squashed_migration)
        client.connection.commit()
        write_line("Migrations marked as squashed in the database.")
        write_line(f"Squashed migration {squashed_migration.name} applied in the database.")

    client.changelog.migrations = [m for m in client.changelog.migrations if m.name not in to_squash]
    client.changelog.save()
    write_line(f"Changelog updated: removed {len(to_squash)} migration(s), added {squashed_migration.name}")

    return 0


def cmd_show(client: SqlClient[Any], list_mode: bool = False, validate_sql: bool = False) -> int:
    status_map = client.retrieve_migration_statuses()
    status_count = Counter({status: 0 for status in MigrationStatus})
    status_count.update(status_map.values())

    write_line("\nMigration Precedence DAG:\n")
    write_line(f"{'Migration File':<40} | {'Status'}")
    write_line("-" * 60)

    if list_mode:
        client.changelog.print_list(status_map)
    else:
        client.changelog.print_dag(status_map=status_map)

    write_line("\nSummary:")
    for status, label in {
        MigrationStatus.APPLIED: "Applied",
        MigrationStatus.NOT_APPLIED: "Not Applied",
        MigrationStatus.REMOVED: "Removed",
        MigrationStatus.CONFLICT: "Conflict",
    }.items():
        write_line(f"  {label:<12}: {STATUS_COLORS[status]}{status_count[status]}{STATUS_COLORS['reset']}")

    not_applied = status_count[MigrationStatus.NOT_APPLIED]
    if not_applied > 0:
        write_line(f"\n→ {not_applied} migration(s) pending. Run `migrateit migrate` to apply them.")
    removed_count = status_count[MigrationStatus.REMOVED]
    if removed_count > 0:
        write_line(f"\n⚠ {removed_count} migration(s) found in database but missing from changelog.")
    conflict_count = status_count[MigrationStatus.CONFLICT]
    if conflict_count > 0:
        write_line(f"\n⚠ {conflict_count} migration(s) have hash conflicts between file and database.")

    if validate_sql:
        write_line("\nValidating SQL migrations...")
        has_err = False
        for migration in client.changelog.migrations:
            err = client.validate_sql_syntax(migration)
            if err:
                has_err = True
                pretty_print_sql_error(err[0], err[1])
        msg = "failed. Please fix the errors above." if has_err else "passed. No errors found."
        write_line("SQL validation " + msg)
    return 0
