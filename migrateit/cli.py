import argparse
import os
from pathlib import Path

import psycopg2

from migrateit.clients import PsqlClient, SqlClient
from migrateit.models import (
    ChangelogFile,
    MigrateItConfig,
    Migration,
    MigrationStatus,
    SupportedDatabase,
)
from migrateit.utils import STATUS_COLORS, print_dag

ROOT_DIR = os.getenv("MIGRATIONS_DIR", "migrateit")


def cmd_init(table_name: str, migrations_dir: Path, migrations_file: Path, database: SupportedDatabase) -> None:
    print("\tCreating migrations file")
    changelog = ChangelogFile.create_file(migrations_file, database)
    print("\tCreating migrations folder")
    Migration.create_directory(migrations_dir)
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
    Migration.create_new(changelog=client.changelog, migrations_dir=client.migrations_dir, name=args.name)


def cmd_run(client: SqlClient, args) -> None:
    assert client.is_migrations_table_created(), f"Migrations table={client.table_name} does not exist"

    # validate the changelog before doing anything
    _ = client.retrieve_migrations()

    target_migration = client.changelog.get_migration_by_name(args.name) if args.name else None
    migration_plan = client.changelog.build_migration_plan(target_migration)

    if not migration_plan:
        print("No migrations to apply.")
        return
    assert migration_plan[0].initial, "Initial migration not found in migration plan"

    for migration in migration_plan:
        if not client.is_migration_applied(migration):
            print(f"Applying migration: {migration.name}")
            client.apply_migration(migration, fake=args.fake)
    client.connection.commit()


def cmd_status(client: SqlClient, *_) -> None:
    migrations = client.retrieve_migrations()
    status_count = {status: 0 for status in MigrationStatus}

    for _, status in migrations.values():
        status_count[status] += 1

    root, children = client.changelog.graph
    status_map = {m.name: status for m, status in migrations.values()}

    print("\nMigration Precedence DAG:\n")
    print(f"{'Migration File':<40} | {'Status'}")
    print("-" * 60)
    print_dag(root, children, status_map)

    print("\nSummary:")
    for status, label in {
        MigrationStatus.APPLIED: "Applied",
        MigrationStatus.NOT_APPLIED: "Not Applied",
        MigrationStatus.REMOVED: "Removed",
        MigrationStatus.CONFLICT: "Conflict",
    }.items():
        print(f"  {label:<12}: {STATUS_COLORS[status]}{status_count[status]}{STATUS_COLORS['reset']}")


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

    parser = argparse.ArgumentParser(prog="migrateit", description="Migration tool")
    subparsers = parser.add_subparsers(dest="command")

    # migrateit init
    parser_init = subparsers.add_parser("init", help="Initialize the migration directory and database")
    parser_init.add_argument(
        "database",
        choices=[db.value for db in SupportedDatabase],
        help=f"Database to be used for migrations (choices: {', '.join(db.value for db in SupportedDatabase)})",
    )
    parser_init.set_defaults(func=cmd_init)

    # migrateit init
    parser_init = subparsers.add_parser("newmigration", help="Create a new migration")
    parser_init.add_argument("name", help="Name of the new migration")
    parser_init.set_defaults(func=cmd_new)

    # migrateit run
    parser_run = subparsers.add_parser("migrate", help="Run migrations")
    parser_run.add_argument("name", type=str, nargs="?", default=None, help="Name of the migration to run")
    parser_run.add_argument("--fake", action="store_true", default=False, help="Fakes the migration marking it as ran.")
    parser_run.set_defaults(func=cmd_run)

    # migrateit status
    parser_status = subparsers.add_parser("showmigrations", help="Show migration status")
    parser_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    if hasattr(args, "func"):
        if args.command == "init":
            cmd_init(
                table_name=os.getenv("MIGRATIONS_TABLE", "MIGRATEIT_CHANGELOG"),
                migrations_dir=Path(ROOT_DIR) / "migrations",
                migrations_file=Path(ROOT_DIR) / "changelog.json",
                database=SupportedDatabase(args.database),
            )
            return

        # TODO: add support for other databases
        db_url = PsqlClient.get_environment_url()
        with psycopg2.connect(db_url) as conn:
            root = Path(ROOT_DIR)
            config = MigrateItConfig(
                table_name=os.getenv("MIGRATIONS_TABLE", "MIGRATEIT_CHANGELOG"),
                migrations_dir=root / "migrations",
                changelog=ChangelogFile.load_file(root / "changelog.json"),
            )
            client = PsqlClient(conn, config)
            args.func(client, args)
    else:
        parser.print_help()
