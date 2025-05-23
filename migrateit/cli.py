import argparse
import os
from pathlib import Path

import psycopg2

from migrateit.clients import PsqlClient, SqlClient
from migrateit.models import (
    MigrateItConfig,
    MigrationStatus,
    SupportedDatabase,
)
from migrateit.tree import (
    build_migration_plan,
    build_migrations_tree,
    create_changelog_file,
    create_migration_directory,
    create_new_migration,
    load_changelog_file,
)
from migrateit.utils import STATUS_COLORS, pretty_print_sql_error, print_dag

ROOT_DIR = os.getenv("MIGRATEIT_MIGRATIONS_DIR", "migrateit")
DATABASE = os.getenv("MIGRATEIT_DATABASE")


def cmd_init(table_name: str, migrations_dir: Path, migrations_file: Path, database: SupportedDatabase) -> None:
    print("\tCreating migrations file")
    changelog = create_changelog_file(migrations_file, database)
    print("\tCreating migrations folder")
    create_migration_directory(migrations_dir)
    print("\tInitializing migration database")
    db_url = PsqlClient.get_environment_url()
    with psycopg2.connect(db_url) as conn:
        config = MigrateItConfig(
            table_name=table_name,
            migrations_dir=migrations_dir,
            changelog=changelog,
        )
        PsqlClient(conn, config).create_migrations_table()


def cmd_new(client: SqlClient, args) -> None:
    assert client.is_migrations_table_created(), f"Migrations table={client.table_name} does not exist"
    create_new_migration(changelog=client.changelog, migrations_dir=client.migrations_dir, name=args.name)


def cmd_run(client: SqlClient, args) -> None:
    assert client.is_migrations_table_created(), f"Migrations table={client.table_name} does not exist"
    is_fake, is_rollback, is_hash_update = args.fake, args.rollback, args.update_hash
    target_migration = client.changelog.get_migration_by_name(args.name) if args.name else None

    if is_hash_update:
        assert target_migration, "Hash update requires a target migration"
        print(f"Updating hash for migration: {target_migration.name}")
        client.update_migration_hash(target_migration)
        return

    statuses = client.retrieve_migration_statuses()
    if is_fake:
        # we don't validate fake migrations
        assert target_migration, "Fake migration requires a target migration"
        print(f"{'Applying' if not is_rollback else 'Rollback'} migration: {target_migration.name}")
        client.apply_migration(target_migration, is_fake=is_fake, is_rollback=is_rollback)
        client.connection.commit()
        return

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
        print("Nothing to apply.")
        return

    for migration in migration_plan:
        print(f"{'Applying' if not is_rollback else 'Rollback'} migration: {migration.name}")
        client.apply_migration(migration, is_rollback=is_rollback)

    client.connection.commit()


def cmd_status(client: SqlClient, args) -> None:
    validate_sql = args.validate_sql

    migrations = build_migrations_tree(client.changelog)
    status_map = client.retrieve_migration_statuses()
    status_count = {status: 0 for status in MigrationStatus}

    for status in status_map.values():
        status_count[status] += 1

    print("\nMigration Precedence DAG:\n")
    print(f"{'Migration File':<40} | {'Status'}")
    print("-" * 60)
    print_dag(next(iter(migrations)), migrations, status_map)

    print("\nSummary:")
    for status, label in {
        MigrationStatus.APPLIED: "Applied",
        MigrationStatus.NOT_APPLIED: "Not Applied",
        MigrationStatus.REMOVED: "Removed",
        MigrationStatus.CONFLICT: "Conflict",
    }.items():
        print(f"  {label:<12}: {STATUS_COLORS[status]}{status_count[status]}{STATUS_COLORS['reset']}")

    if validate_sql:
        print("\nValidating SQL migrations...")
        msg = "SQL validation passed. No errors found."
        for migration in client.changelog.migrations:
            err = client.validate_sql_sintax(migration)
            if err:
                msg = "\nSQL validation failed. Please fix the errors above."
                pretty_print_sql_error(err[0], err[1])
        print(msg)


# TODO: add support for other databases
def _get_connection():
    match DATABASE:
        case SupportedDatabase.POSTGRES.value:
            db_url = PsqlClient.get_environment_url()
            return psycopg2.connect(db_url)
        case _:
            raise NotImplementedError(f"Database {DATABASE} is not supported")


def main():
    print(r"""
##########################################
 __  __ _                 _       ___ _
|  \/  (_) __ _ _ __ __ _| |_ ___|_ _| |_
| |\/| | |/ _` | '__/ _` | __/ _ \| || __|
| |  | | | (_| | | | (_| | ||  __/| || |_
|_|  |_|_|\__, |_|  \__,_|\__\___|___|\__|
          |___/
##########################################
          """)

    assert DATABASE in [db.value for db in SupportedDatabase], (
        f"Database {DATABASE} is not supported. Supported databases are: {[db.value for db in SupportedDatabase]}"
    )

    parser = argparse.ArgumentParser(prog="migrateit", description="Migration tool")
    subparsers = parser.add_subparsers(dest="command")

    # migrateit init
    parser_init = subparsers.add_parser("init", help="Initialize the migration directory and database")
    parser_init.set_defaults(func=cmd_init)

    # migrateit init
    parser_init = subparsers.add_parser("newmigration", help="Create a new migration")
    parser_init.add_argument("name", help="Name of the new migration")
    parser_init.set_defaults(func=cmd_new)

    # migrateit run
    parser_run = subparsers.add_parser("migrate", help="Run migrations")
    parser_run.add_argument("name", type=str, nargs="?", default=None, help="Name of the migration to run")
    parser_run.add_argument("--fake", action="store_true", default=False, help="Fakes the migration marking it as ran.")
    parser_run.add_argument(
        "--update-hash", action="store_true", default=False, help="Update the hash of the migration."
    )
    parser_run.add_argument(
        "--rollback",
        action="store_true",
        default=False,
        help="Undo the given migration and all its applied childs.",
    )
    parser_run.set_defaults(func=cmd_run)

    # migrateit status
    parser_status = subparsers.add_parser("showmigrations", help="Show migration status")
    parser_status.add_argument(
        "--validate-sql",
        action="store_true",
        default=False,
        help="Validate SQL migration sintax.",
    )
    parser_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    if hasattr(args, "func"):
        if args.command == "init":
            cmd_init(
                table_name=os.getenv("MIGRATEIT_MIGRATIONS_TABLE", "MIGRATEIT_CHANGELOG"),
                migrations_dir=Path(ROOT_DIR) / "migrations",
                migrations_file=Path(ROOT_DIR) / "changelog.json",
                database=SupportedDatabase(DATABASE),
            )
            return

        root = Path(ROOT_DIR)
        config = MigrateItConfig(
            table_name=os.getenv("MIGRATIONS_TABLE", "MIGRATEIT_CHANGELOG"),
            migrations_dir=root / "migrations",
            changelog=load_changelog_file(root / "changelog.json"),
        )

        with _get_connection() as conn:
            client = PsqlClient(conn, config)
            args.func(client, args)
    else:
        parser.print_help()
