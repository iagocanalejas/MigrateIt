from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from migrateit.clients._client import SqlClient
from migrateit.clients.sqlite import SqliteClient
from migrateit.models.config import MigrateItConfig, SupportedDatabase
from migrateit.models.migration import Migration
from migrateit.models.repository import ChangelogRepository
from tests.unit.conftest import INITIAL_MIGRATION, TEST_MIGRATIONS_TABLE


def _get_query_rows(client: SqlClient[Any], query: str) -> list[tuple[Any, ...]]:
    match client.database:
        case (
            SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB | SupportedDatabase.ORACLE
        ):
            with client.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        case SupportedDatabase.SQLITE:
            return client.connection.execute(query).fetchall()
        case _:  # pragma: no cover
            raise NotImplementedError


def _migration_is_applied(client: SqlClient[Any], migration_name: str) -> bool:
    table_name = client._q(TEST_MIGRATIONS_TABLE)
    match client.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE migration_name = %s",
                    (migration_name,),
                )
                result = cursor.fetchone()
                assert result is not None
                return bool(result[0])
        case SupportedDatabase.ORACLE:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE migration_name = :1",
                    (migration_name,),
                )
                result = cursor.fetchone()
                assert result is not None
                return bool(result[0])
        case SupportedDatabase.SQLITE:
            cursor = client.connection.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE migration_name = ?",
                (migration_name,),
            )
            result = cursor.fetchone()
            assert result is not None
            return bool(result[0])
        case _:  # pragma: no cover
            raise NotImplementedError


def _table_exists(client: SqlClient[Any], table_name: str) -> bool:
    match client.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
                    (table_name,),
                )
                result = cursor.fetchone()
                assert result is not None
                return result[0]
        case SupportedDatabase.ORACLE:
            with client.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM user_tables WHERE UPPER(table_name) = UPPER(:1)",
                    (table_name,),
                )
                result = cursor.fetchone()
                assert result is not None
                return bool(result[0])
        case SupportedDatabase.SQLITE:
            cursor = client.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            result = cursor.fetchone()
            return result is not None
        case _:  # pragma: no cover
            raise NotImplementedError


def _drop_test_table(client: SqlClient[Any], table_name: str) -> None:
    table_name = client._q(table_name)
    match client.database:
        case SupportedDatabase.POSTGRES | SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        case SupportedDatabase.ORACLE:
            import oracledb

            with client.connection.cursor() as cursor:
                try:
                    cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE")
                except oracledb.DatabaseError as e:  # pragma: no cover[safety]
                    (error,) = e.args
                    if error.code != 942:
                        raise
                cursor.execute("PURGE RECYCLEBIN")
        case SupportedDatabase.SQLITE:
            client.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        case _:  # pragma: no cover
            raise NotImplementedError
    client.connection.commit()


def _client_with_mocked_connection(temp_dir: Path) -> SqlClient[Any]:
    changelog = ChangelogRepository.create(database=SupportedDatabase.POSTGRES)
    changelog.migrations.append(Migration(name=INITIAL_MIGRATION, initial=True, parents=()))
    ChangelogRepository.save(changelog)

    config = MigrateItConfig(changelog=ChangelogRepository.load())
    return SqliteClient(MagicMock(), config)
