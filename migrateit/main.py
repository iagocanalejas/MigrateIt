import argparse
from datetime import datetime
from pathlib import Path

import psycopg2

import migrateit.constants as C
from migrateit import cli as commands
from migrateit.clients.psql import PsqlClient
from migrateit.models import MigrateItConfig, SupportedDatabase
from migrateit.reporters import FatalError, error_handler, logging_handler, print_logo
from migrateit.tree import load_changelog_file


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
    _cmd_init(subparsers)
    _cmd_new(subparsers)
    _cmd_migrate(subparsers)
    _cmd_rollback(subparsers)
    _cmd_squash(subparsers)
    _cmd_show(subparsers)
    args = parser.parse_args()

    print_logo()
    with error_handler(), logging_handler(True):
        if hasattr(args, "func"):
            root = Path(C.MIGRATEIT_ROOT_DIR)
            if args.command == "init":
                if args.database not in [db.value for db in SupportedDatabase]:
                    raise FatalError(f"Unsupported database type: {args.database}.")
                return commands.cmd_init(
                    table_name=C.MIGRATEIT_MIGRATIONS_TABLE,
                    migrations_dir=root / "migrations",
                    migrations_file=root / "changelog.json",
                    database=SupportedDatabase(args.database),
                )

            changelog = load_changelog_file(root / "changelog.json")
            config = MigrateItConfig(
                table_name=C.MIGRATEIT_MIGRATIONS_TABLE,
                migrations_dir=root / "migrations",
                changelog=changelog,
            )
            with _get_connection(changelog.database) as conn:
                client = PsqlClient(conn, config)
                if args.command == "new":
                    return commands.cmd_new(
                        client,
                        name=args.name,
                        dependencies=args.dependecies,
                        no_edit=args.no_edit,
                    )
                elif args.command == "show":
                    return commands.cmd_show(
                        client,
                        list_mode=args.list,
                        validate_sql=args.validate_sql,
                    )
                elif args.command == "migrate":
                    return commands.cmd_run(
                        client,
                        args.name,
                        is_fake=args.fake,
                        is_hash_update=args.update_hash,
                    )
                elif args.command == "rollback":
                    return commands.cmd_run(
                        client,
                        args.name,
                        is_fake=args.fake,
                        is_rollback=True,
                    )
                elif args.command == "squash":
                    return commands.cmd_squash(
                        client,
                        start_migration=args.start_migration,
                        end_migration=args.end_migration,
                        name=args.name,
                    )
                else:
                    raise NotImplementedError(f"Command {args.command} not implemented.")
        else:
            parser.print_help()
            return 1


def _cmd_init(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("init", help="Initialize the migration directory and database")
    parser.add_argument("database", help="Database type to use", choices=[db.value for db in SupportedDatabase])
    parser.set_defaults(func=commands.cmd_init)
    return parser


def _cmd_new(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("new", help="Create a new migration")
    parser.add_argument(
        "name",
        type=str,
        nargs="?",
        default=f"auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        help="Name of the new migration",
    )
    parser.add_argument(
        "-d",
        "--dependecies",
        nargs="*",
        help="List of migration names that this migration depends on.",
    )
    parser.add_argument(
        "--no-edit",
        action="store_true",
        default=False,
        help="Avoid opening the migration file in an editor after creation.",
    )
    parser.set_defaults(func=commands.cmd_new)
    return parser


def _cmd_migrate(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("migrate", help="Run migrations")
    parser.add_argument("name", type=str, nargs="?", default=None, help="Name of the migration to run")
    parser.add_argument("--fake", action="store_true", default=False, help="Fakes the migration marking it as ran.")
    parser.add_argument(
        "--update-hash",
        action="store_true",
        default=False,
        help="Update the hash of the migration.",
    )
    parser.set_defaults(func=commands.cmd_run)
    return parser


def _cmd_rollback(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("rollback", help="Rollback migrations")
    parser.add_argument("name", type=str, nargs="?", default=None, help="Name of the migration to run")
    parser.add_argument(
        "--fake",
        action="store_true",
        default=False,
        help="Fakes the migration marking it as ran.",
    )
    parser.set_defaults(func=commands.cmd_run)
    return parser


def _cmd_squash(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("squash", help="Squash migrations into a single file")
    parser.add_argument(
        "start_migration",
        type=str,
        help="Name of the first migration to squash from (inclusive).",
    )
    parser.add_argument(
        "end_migration",
        type=str,
        nargs="?",
        help="Name of the last migration to squash to (inclusive). If not provided, the last migration is used.",
    )
    parser.add_argument(
        "-n",
        "--name",
        type=str,
        help="Name of the new squashed migration file. If not provided, a default name will be generated.",
    )
    parser.set_defaults(func=commands.cmd_squash)
    return parser


def _cmd_show(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("show", help="Show migration status")
    parser.add_argument(
        "-l",
        "--list",
        action="store_true",
        default=False,
        help="Display migrations in a list format.",
    )
    parser.add_argument(
        "--validate-sql",
        action="store_true",
        default=False,
        help="Validate SQL migration syntax.",
    )
    parser.set_defaults(func=commands.cmd_show)
    return parser


# TODO: add support for other databases
def _get_connection(database: SupportedDatabase):
    match database:
        case SupportedDatabase.POSTGRES:
            db_url = PsqlClient.get_environment_url()
            conn = psycopg2.connect(db_url)
            conn.autocommit = False
            return conn
        case _:
            raise NotImplementedError(f"Database {database} is not supported")


if __name__ == "__main__":
    raise SystemExit(main())
