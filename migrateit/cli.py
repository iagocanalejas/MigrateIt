import os
import platform
import shlex
import subprocess
from pathlib import Path

from migrateit.clients import PsqlClient, SqlClient
from migrateit.models import (
    MigrationStatus,
    SupportedDatabase,
)
from migrateit.reporters import STATUS_COLORS, pretty_print_sql_error, print_dag, print_list, write_line
from migrateit.tree import (
    build_migration_plan,
    build_migrations_tree,
    create_changelog_file,
    create_migration_directory,
    create_new_migration,
    find_path,
    retrieve_migration_sqls,
    save_changelog_file,
    write_into_migration_file,
)


def cmd_init(table_name: str, migrations_dir: Path, migrations_file: Path, database: SupportedDatabase) -> int:
    write_line(f"\tCreating migrations file: {migrations_file}")
    changelog = create_changelog_file(migrations_file, database)

    write_line(f"\tCreating migrations directory: {migrations_dir}")
    create_migration_directory(migrations_dir)

    write_line(f"\tCreating migration for table: {table_name}")
    migration = create_new_migration(changelog=changelog, migrations_dir=migrations_dir, name="migrateit")
    match database:
        case SupportedDatabase.POSTGRES:
            sql, rollback = PsqlClient.create_migrations_table_str(table_name=table_name)
        case _:
            raise NotImplementedError(f"Database {database} is not supported yet")

    write_into_migration_file(Path(migrations_dir / migration.name), sql=sql, rollback=rollback)

    return 0


def cmd_new(
    client: SqlClient,
    name: str,
    dependencies: list[str] | None = None,
    no_edit: bool = False,
) -> int:
    if not client.is_migrations_table_created():
        raise ValueError(f"Migrations table={client.table_name} does not exist. Please run `init` & `migrate` first.")

    migration = create_new_migration(
        changelog=client.changelog,
        migrations_dir=client.migrations_dir,
        name=name,
        dependencies=dependencies,
    )

    if no_edit:
        return 0

    editor = os.getenv("EDITOR", "notepad.exe" if platform.system() == "Windows" else "vim")
    cmd = shlex.split(editor) + [str(client.migrations_dir / migration.name)]
    return subprocess.call(cmd)


def cmd_run(
    client: SqlClient,
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
        return 0

    statuses = client.retrieve_migration_statuses()
    if is_fake:
        if not target_migration:
            raise ValueError("Fake migration requires a target migration name")
        if target_migration.initial:
            raise ValueError("Cannot fake the initial migration")
        write_line(f"{'Faking' if not is_rollback else 'Faking rollback for'} migration: {target_migration.name}")
        client.apply_migration(target_migration, is_fake=is_fake, is_rollback=is_rollback)
        client.connection.commit()
        return 0

    if is_rollback and not target_migration:
        raise ValueError("Rollback requires a target migration name")
    client.validate_migrations(statuses)

    migration_plan = build_migration_plan(
        client.changelog,
        migration_tree=build_migrations_tree(client.changelog),
        statuses_map=statuses,
        target_migration=target_migration,
        is_rollback=is_rollback,
    )

    if not migration_plan:
        write_line("Nothing to do.")
        return 0

    for migration in migration_plan:
        write_line(f"{'Applying' if not is_rollback else 'Rolling back'} migration: {migration.name}")
        client.apply_migration(migration, is_rollback=is_rollback)

    client.connection.commit()
    return 0


def cmd_squash(
    client: SqlClient,
    start_migration: str,
    end_migration: str | None = None,
    name: str | None = None,
) -> int:
    if not end_migration:
        end_migration = client.changelog.migrations[-1].name

    start_migration = client.changelog.get_migration_by_name(start_migration).name
    end_migration = client.changelog.get_migration_by_name(end_migration).name
    write_line(f"Squashing migrations from {start_migration} to {end_migration}.")

    to_squash = find_path(build_migrations_tree(client.changelog), start_migration, end_migration)
    write_line(f"Following migrations will be squashed: {', '.join(to_squash)}")
    if not to_squash:
        raise ValueError(f"No path found from {start_migration} to {end_migration}.")
    if any(m.initial for m in (client.changelog.get_migration_by_name(m) for m in to_squash)):
        raise ValueError("Cannot squash initial migrations.")

    statuses = client.retrieve_migration_statuses()
    if not all(statuses[m] == statuses[to_squash[0]] for m in to_squash):
        raise ValueError("Cannot squash migrations that are not in the same state.")

    squashed_migration = create_new_migration(
        changelog=client.changelog,
        migrations_dir=client.migrations_dir,
        name=name if name else f"squashed_{start_migration}_{end_migration}",
        dependencies=client.changelog.get_migration_by_name(start_migration).parents,
    )

    for migration_name in to_squash:
        migration = client.changelog.get_migration_by_name(migration_name)
        write_line(f"Squashing migration: {migration.name}")
        sql, rollback = retrieve_migration_sqls(client.migrations_dir / migration.name)
        write_into_migration_file(client.migrations_dir / squashed_migration.name, sql=sql, rollback=rollback)

    write_line(f"Squashed migration created: {squashed_migration.name}")

    if all(statuses[m] == MigrationStatus.APPLIED for m in to_squash):
        client.squash_migrations(to_squash, squashed_migration)
        client.connection.commit()
        write_line("Migrations marked as squashed in the database.")
        write_line(f"Squashed migration {squashed_migration.name} applied in the database.")

    client.changelog.migrations = [m for m in client.changelog.migrations if m.name not in to_squash]
    save_changelog_file(client.changelog)
    write_line("Changelog file updated")

    return 0


def cmd_show(client: SqlClient, list_mode: bool = False, validate_sql: bool = False) -> int:
    migrations = build_migrations_tree(client.changelog)
    status_map = client.retrieve_migration_statuses()
    status_count = {status: 0 for status in MigrationStatus}

    for status in status_map.values():
        status_count[status] += 1

    write_line("\nMigration Precedence DAG:\n")
    write_line(f"{'Migration File':<40} | {'Status'}")
    write_line("-" * 60)

    if list_mode:
        print_list(migrations, status_map)
    else:
        print_dag(next(iter(migrations)), migrations, status_map)

    write_line("\nSummary:")
    for status, label in {
        MigrationStatus.APPLIED: "Applied",
        MigrationStatus.NOT_APPLIED: "Not Applied",
        MigrationStatus.REMOVED: "Removed",
        MigrationStatus.CONFLICT: "Conflict",
    }.items():
        write_line(f"  {label:<12}: {STATUS_COLORS[status]}{status_count[status]}{STATUS_COLORS['reset']}")

    if validate_sql:
        write_line("\nValidating SQL migrations...")
        msg = "SQL validation passed. No errors found."
        for migration in client.changelog.migrations:
            err = client.validate_sql_syntax(migration)
            if err:
                msg = "\nSQL validation failed. Please fix the errors above."
                pretty_print_sql_error(err[0], err[1])
        write_line(msg)
    return 0
