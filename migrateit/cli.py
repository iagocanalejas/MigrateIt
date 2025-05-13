import argparse
import os

import psycopg2

from migrateit.clients import PsqlClient, SqlClient
from migrateit.files import create_migrations_dir, create_migrations_file, create_new_migration
from migrateit.models import MigrateItConfig

DB_URL = PsqlClient.get_environment_url()
ROOT_DIR = os.getenv("MIGRATIONS_DIR", "db")


def cmd_init(client: SqlClient, *args):
    print("\tCreating migrations file")
    create_migrations_file(client.migrations_file)
    print("\tCreating migrations folder")
    create_migrations_dir(client.migrations_dir)
    print("\tInitializing migration database")
    client.create_migrations_table()


def cmd_new(client: SqlClient, args):
    assert client.check_migrations_table_exist(), f"Migrations table={client.table_name} does not exist"

    create_new_migration(client.config, args.name)


def cmd_run(client: SqlClient):
    assert client.check_migrations_table_exist(), f"Migrations table={client.table_name} does not exist"


def cmd_status(client: SqlClient):
    assert client.check_migrations_table_exist(), f"Migrations table={client.table_name} does not exist"


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
            config = MigrateItConfig(
                table_name=os.getenv("MIGRATIONS_TABLE", "MI_CHANGELOG"),
                migrations_dir=os.path.join(ROOT_DIR, "migrations"),
                migrations_file=os.path.join(ROOT_DIR, "changelog.json"),
            )
            client = PsqlClient(conn, config)
            args.func(client, args)
    else:
        parser.print_help()
