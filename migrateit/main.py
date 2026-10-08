import argparse
from datetime import datetime
from pathlib import Path

import migrateit.constants as C
from migrateit import cmd as commands
from migrateit.clients._client import get_client
from migrateit.models.changelog import SupportedDatabase, load_changelog_file
from migrateit.models.config import MigrateItConfig
from migrateit.models.connection import get_connection
from migrateit.reporters import FatalError, error_handler, logging_handler, print_logo
from migrateit.reporters.logs import logger


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

    def _add_cmd(name: str, *, help: str) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(name, help=help)
        return parser

    _cmd_init(_add_cmd("init", help="Initialize the migration directory and database"))
    _cmd_export(_add_cmd("export", help="Export the current database schema to a migration file"))
    _cmd_new(_add_cmd("new", help="Create a new migration"))
    _cmd_migrate(_add_cmd("migrate", help="Run migrations"))
    _cmd_rollback(_add_cmd("rollback", help="Rollback migrations"))
    _cmd_squash(_add_cmd("squash", help="Squash migrations into a single file"))
    _cmd_show(_add_cmd("show", help="Show migration status"))
    _cmd_drop(_add_cmd("drop", help="Drop and rollback a migration"))
    args = parser.parse_args()

    print_logo()
    with error_handler(), logging_handler(True):
        if hasattr(args, "func"):
            logger.debug("Running command: %s", args.command)
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
            with get_connection(changelog.database) as conn:
                logger.debug("Connected to database: %s", changelog.database.value)
                client = get_client(config, connection=conn)

                if args.command == "export":
                    return commands.cmd_export(client, args.name)
                elif args.command == "new":
                    return commands.cmd_new(
                        client,
                        name=args.name,
                        dependencies=tuple(args.dependencies),
                        no_edit=args.no_edit,
                        interactive=args.interactive,
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
                        is_plan_only=args.dry_run,
                    )
                elif args.command == "rollback":
                    return commands.cmd_run(
                        client,
                        args.name,
                        is_fake=args.fake,
                        is_rollback=True,
                        is_plan_only=args.dry_run,
                    )
                elif args.command == "squash":
                    return commands.cmd_squash(
                        client,
                        start_migration=args.start_migration,
                        end_migration=args.end_migration,
                        name=args.name,
                    )
                elif args.command == "drop":
                    return commands.cmd_drop(client, args.name)
                else:
                    raise NotImplementedError(f"Command {args.command} not implemented.")
        else:
            parser.print_help()
            return 1


def _cmd_init(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("database", help="Database type to use", choices=[db.value for db in SupportedDatabase])
    parser.set_defaults(func=commands.cmd_init)
    return parser


def _cmd_export(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "name",
        type=str,
        nargs="?",
        help="Name of the migration containing the database SQL dump.",
    )
    parser.set_defaults(func=commands.cmd_export)
    return parser


def _cmd_new(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    def _auto_name() -> str:
        return f"auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    parser.add_argument(
        "name",
        type=str,
        nargs="?",
        default=_auto_name,
        help="Name of the new migration.",
    )
    parser.add_argument(
        "-d",
        "--dependencies",
        nargs="*",
        help="List of migration names that this migration depends on.",
    )
    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        default=False,
        help="Allow to interactively select the migration dependencies.",
    )
    parser.add_argument(
        "--no-edit",
        action="store_true",
        default=False,
        help="Avoid opening the migration file in an editor after creation.",
    )
    parser.set_defaults(func=commands.cmd_new)
    return parser


def _cmd_migrate(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("name", type=str, nargs="?", default=None, help="Name of the migration to run")
    parser.add_argument(
        "--fake",
        action="store_true",
        default=False,
        help="Fakes the migration marking it as ran.",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        default=False,
        help="Dry run the migration without applying it.",
    )
    parser.add_argument(
        "--update-hash",
        action="store_true",
        default=False,
        help="Update the hash of the migration.",
    )
    parser.set_defaults(func=commands.cmd_run)
    return parser


def _cmd_rollback(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("name", type=str, nargs="?", default=None, help="Name of the migration to run")
    parser.add_argument(
        "--fake",
        action="store_true",
        default=False,
        help="Fakes the migration marking it as ran.",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        default=False,
        help="Dry run the migration without applying it.",
    )
    parser.set_defaults(func=commands.cmd_run)
    return parser


def _cmd_squash(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
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


def _cmd_show(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
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


def _cmd_drop(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("name", type=str, default=None, help="Name of the migration to drop.")
    parser.set_defaults(func=commands.cmd_drop)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
