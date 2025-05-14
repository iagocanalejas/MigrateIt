import argparse
import os
from pathlib import Path

import psycopg2

from migrateit.clients import PsqlClient, SqlClient
from migrateit.models import MigrateItConfig, Migration, MigrationsFile, MigrationStatus

DB_URL = PsqlClient.get_environment_url()
ROOT_DIR = os.getenv("MIGRATIONS_DIR", "migrateit")


def cmd_init(client: "SqlClient", *_):
    print("\tCreating migrations file")
    MigrationsFile.create_file(client.migrations_file)
    print("\tCreating migrations folder")
    Migration.create_directory(client.migrations_dir)
    print("\tInitializing migration database")
    client.create_migrations_table()


def cmd_new(client: SqlClient, args):
    assert client.is_migrations_table_created(), f"Migrations table={client.table_name} does not exist"
    Migration.create_new(client.migrations_dir, client.migrations_file, args.name)


def cmd_run(client: SqlClient, *_):
    assert client.is_migrations_table_created(), f"Migrations table={client.table_name} does not exist"
    changelog = MigrationsFile.load_file(client.migrations_file)

    for migration in changelog.migrations:
        if not client.is_migration_applied(migration):
            print(f"Applying migration: {migration.name}")
            client.apply_migration(changelog, migration)
    client.connection.commit()


def cmd_status(client: SqlClient, *_):
    changelog = MigrationsFile.load_file(client.migrations_file)
    migrations = client.retrieve_migrations(changelog)

    print("\nMigration Status:\n")
    print(f"{'Migration File':<40} | {'Status'}")
    print("-" * 60)

    status_count = {
        MigrationStatus.APPLIED: 0,
        MigrationStatus.NOT_APPLIED: 0,
        MigrationStatus.REMOVED: 0,
        MigrationStatus.CONFLICT: 0,
    }

    for migration, status in migrations:
        status_count[status] += 1

        status_str = {
            MigrationStatus.APPLIED: "Applied",
            MigrationStatus.NOT_APPLIED: "Not Applied",
            MigrationStatus.REMOVED: "Removed",
            MigrationStatus.CONFLICT: "Conflict",
        }[status]

        print(f"{migration.name:<40} | {status_str}")

    print("\nSummary:")
    for key, label in {
        MigrationStatus.APPLIED: "Applied",
        MigrationStatus.NOT_APPLIED: "Not Applied",
        MigrationStatus.REMOVED: "Removed",
        MigrationStatus.CONFLICT: "Conflict",
    }.items():
        print(f"  {label:<12}: {status_count[key]}")


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
    parser_init.set_defaults(func=cmd_init)

    # migrateit init
    parser_init = subparsers.add_parser("newmigration", help="Create a new migration")
    parser_init.add_argument("name", help="Name of the new migration")
    parser_init.set_defaults(func=cmd_new)

    # migrateit run
    parser_run = subparsers.add_parser("migrate", help="Run migrations")
    parser_run.set_defaults(func=cmd_run)

    # migrateit status
    parser_status = subparsers.add_parser("showmigrations", help="Show migration status")
    parser_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    if hasattr(args, "func"):
        with psycopg2.connect(DB_URL) as conn:
            root = Path(ROOT_DIR)
            config = MigrateItConfig(
                table_name=os.getenv("MIGRATIONS_TABLE", "MIGRATEIT_CHANGELOG"),
                migrations_dir=root / "migrations",
                migrations_file=root / "changelog.json",
            )
            client = PsqlClient(conn, config)
            args.func(client, args)
    else:
        parser.print_help()
