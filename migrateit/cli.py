from pathlib import Path

import psycopg2

from migrateit.clients import PsqlClient, SqlClient
from migrateit.models import (
    MigrateItConfig,
    MigrationStatus,
    SupportedDatabase,
)
from migrateit.reporters import STATUS_COLORS, pretty_print_sql_error, print_dag, write_line
from migrateit.tree import (
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

    write_line(f"\tCreating migrations table: {table_name}")
    db_url = PsqlClient.get_environment_url()
    with psycopg2.connect(db_url) as conn:
        config = MigrateItConfig(
            table_name=table_name,
            migrations_dir=migrations_dir,
            changelog=changelog,
        )
        PsqlClient(conn, config).create_migrations_table()

    return 0


def cmd_new(client: SqlClient, args) -> int:
    assert client.is_migrations_table_created(), f"Migrations table={client.table_name} does not exist"
    create_new_migration(changelog=client.changelog, migrations_dir=client.migrations_dir, name=args.name)
    return 0


def cmd_run(client: SqlClient, args) -> int:
    assert client.is_migrations_table_created(), f"Migrations table={client.table_name} does not exist"
    is_fake, is_rollback, is_hash_update = args.fake, args.rollback, args.update_hash
    target_migration = client.changelog.get_migration_by_name(args.name) if args.name else None

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


def cmd_status(client: SqlClient, args) -> int:
    validate_sql = args.validate_sql

    migrations = build_migrations_tree(client.changelog)
    status_map = client.retrieve_migration_statuses()
    status_count = {status: 0 for status in MigrationStatus}

    for status in status_map.values():
        status_count[status] += 1

    write_line("\nMigration Precedence DAG:\n")
    write_line(f"{'Migration File':<40} | {'Status'}")
    write_line("-" * 60)
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
            err = client.validate_sql_sintax(migration)
            if err:
                msg = "\nSQL validation failed. Please fix the errors above."
                pretty_print_sql_error(err[0], err[1])
        write_line(msg)
    return 0
