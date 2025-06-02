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
from migrateit.reporters import STATUS_COLORS, pretty_print_sql_error, print_dag, write_line
from migrateit.reporters.output import print_list
from migrateit.tree import (
    ROLLBACK_SPLIT_TAG,
    build_migration_plan,
    build_migrations_tree,
    create_changelog_file,
    create_migration_directory,
    create_new_migration,
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

    path = Path(migrations_dir / migration.name)
    migration_content = path.read_text(encoding="utf-8")
    if ROLLBACK_SPLIT_TAG not in migration_content:
        raise ValueError(f"Migration {migration.name} does not contain a rollback section ({ROLLBACK_SPLIT_TAG})")

    parts = migration_content.split(ROLLBACK_SPLIT_TAG, maxsplit=1)
    new_content = (
        parts[0].rstrip()
        + "\n\n"
        + sql.strip()
        + "\n\n"
        + ROLLBACK_SPLIT_TAG
        + parts[1].rstrip()
        + "\n\n"
        + rollback.strip()
    )

    path.write_text(new_content, encoding="utf-8")

    return 0


def cmd_new(client: SqlClient, name: str, no_edit: bool = False) -> int:
    assert client.is_migrations_table_created(), f"Migrations table={client.table_name} does not exist"
    migration = create_new_migration(changelog=client.changelog, migrations_dir=client.migrations_dir, name=name)

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
        assert target_migration, "Hash update requires a target migration"
        write_line(f"Updating hash for migration: {target_migration.name}")
        client.update_migration_hash(target_migration)
        return 0

    statuses = client.retrieve_migration_statuses()
    if is_fake:
        # we don't validate fake migrations
        assert target_migration, "Fake migration requires a target migration"
        write_line(f"{'Faking' if not is_rollback else 'Faking rollback for'} migration: {target_migration.name}")
        client.apply_migration(target_migration, is_fake=is_fake, is_rollback=is_rollback)
        client.connection.commit()
        return 0

    assert not is_rollback or target_migration, "Rollback requires a target migration"
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
