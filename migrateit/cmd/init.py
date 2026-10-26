from migrateit import constants as C
from migrateit.models.config import SupportedDatabase
from migrateit.models.migration import (
    Migration,
    create_migration_directory,
    write_into_migration_file,
)
from migrateit.models.migration_manager import MigrationManager
from migrateit.models.repository import ChangelogRepository
from migrateit.reporters import write_line
from migrateit.reporters.output import write_line_error


def cmd_init(database: SupportedDatabase) -> int:
    migrations = _get_init_migration_sql(database, C.MIGRATEIT_MIGRATIONS_TABLE)
    if migrations is None:
        write_line_error(f"Database '{database}' is not supported.")
        return 1

    write_line(f"Initializing database '{database.value}'.")
    changelog = ChangelogRepository.create(database)
    write_line(f"\tCreating migrations file: {changelog.path}.")

    write_line(f"\tCreating migrations directory: {Migration.migrations_dir}.")
    create_migration_directory()

    write_line(f"\tCreating migration for table: {C.MIGRATEIT_MIGRATIONS_TABLE}.")
    migration = MigrationManager(changelog).create_new_migration(name="migrateit")

    sql, rollback = migrations
    write_into_migration_file(migration.path, sql=sql, rollback=rollback)

    ChangelogRepository.save(changelog)
    write_line("Initialization complete:")
    write_line(f"\ttable: '{C.MIGRATEIT_MIGRATIONS_TABLE}'")
    write_line(f"\tmigrations: '{Migration.migrations_dir}'")
    write_line(f"\tchangelog: '{changelog.path}'")

    return 0


def _get_init_migration_sql(database: SupportedDatabase, table_name: str) -> tuple[str, str] | None:
    match database:
        case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            from migrateit.clients.mysql import MySqlClient

            return MySqlClient.create_migrations_table_str(table_name=table_name)
        case SupportedDatabase.ORACLE:
            from migrateit.clients.oracle import OracleClient

            return OracleClient.create_migrations_table_str(table_name=table_name)
        case SupportedDatabase.POSTGRES:
            from migrateit.clients.psql import PsqlClient

            return PsqlClient.create_migrations_table_str(table_name=table_name)
        case SupportedDatabase.SQLITE:
            from migrateit.clients.sqlite import SqliteClient

            return SqliteClient.create_migrations_table_str(table_name=table_name)
        case _:
            return None
