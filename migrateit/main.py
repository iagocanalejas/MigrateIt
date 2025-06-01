import argparse
from pathlib import Path

import psycopg2

import migrateit.constants as C
from migrateit import cli as commands
from migrateit.clients.psql import PsqlClient
from migrateit.models import MigrateItConfig, SupportedDatabase
from migrateit.reporters import FatalError, error_handler, logging_handler, print_logo
from migrateit.tree import load_changelog_file


# TODO: add support for other databases
def _get_connection():
    match C.DATABASE:
        case SupportedDatabase.POSTGRES.value:
            db_url = PsqlClient.get_environment_url()
            conn = psycopg2.connect(db_url)
            conn.autocommit = False
            return conn
        case _:
            raise NotImplementedError(f"Database {C.DATABASE} is not supported")


def main() -> int:
    parser = argparse.ArgumentParser(prog="migrateit", description="Migration tool")

    # https://stackoverflow.com/a/8521644/812183
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {C.VERSION}",
    )

    subparsers = parser.add_subparsers(dest="command")

    # migrateit init
    parser_init = subparsers.add_parser("init", help="Initialize the migration directory and database")
    parser_init.set_defaults(func=commands.cmd_init)

    # migrateit init
    parser_init = subparsers.add_parser("new", help="Create a new migration")
    parser_init.add_argument("name", help="Name of the new migration")
    parser_init.set_defaults(func=commands.cmd_new)

    # migrateit run
    parser_run = subparsers.add_parser("migrate", help="Run migrations")
    parser_run.add_argument("name", type=str, nargs="?", default=None, help="Name of the migration to run")
    parser_run.add_argument("--fake", action="store_true", default=False, help="Fakes the migration marking it as ran.")
    parser_run.add_argument(
        "--update-hash",
        action="store_true",
        default=False,
        help="Update the hash of the migration.",
    )
    parser_run.add_argument(
        "--rollback",
        action="store_true",
        default=False,
        help="Undo the given migration and all its applied childs.",
    )
    parser_run.set_defaults(func=commands.cmd_run)

    parser_show = subparsers.add_parser("show", help="Show migration status")
    parser_show.add_argument(
        "-l",
        "--list",
        action="store_true",
        default=False,
        help="Display migrations in a list format.",
    )
    parser_show.add_argument(
        "--validate-sql",
        action="store_true",
        default=False,
        help="Validate SQL migration sintax.",
    )
    parser_show.set_defaults(func=commands.cmd_show)

    args = parser.parse_args()

    print_logo()
    with error_handler(), logging_handler(True):
        if C.DATABASE not in [db.value for db in SupportedDatabase]:
            raise FatalError(
                f"Database {C.DATABASE} is not supported."
                f"Supported databases are: {[db.value for db in SupportedDatabase]}"
            )

        if hasattr(args, "func"):
            if args.command == "init":
                return commands.cmd_init(
                    table_name=C.MIGRATEIT_MIGRATIONS_TABLE,
                    migrations_dir=Path(C.MIGRATEIT_ROOT_DIR) / "migrations",
                    migrations_file=Path(C.MIGRATEIT_ROOT_DIR) / "changelog.json",
                    database=SupportedDatabase(C.DATABASE),
                )

            root = Path(C.MIGRATEIT_ROOT_DIR)
            config = MigrateItConfig(
                table_name=C.MIGRATEIT_MIGRATIONS_TABLE,
                migrations_dir=root / "migrations",
                changelog=load_changelog_file(root / "changelog.json"),
            )
            with _get_connection() as conn:
                client = PsqlClient(conn, config)
                if args.command == "new":
                    return commands.cmd_new(client, args)
                elif args.command == "show":
                    return commands.cmd_show(
                        client,
                        list_mode=args.list,
                        validate_sql=args.validate_sql,
                    )
                elif args.command == "migrate":
                    return commands.cmd_run(client, args)
                else:
                    raise NotImplementedError(f"Command {args.command} not implemented.")
        else:
            parser.print_help()
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
