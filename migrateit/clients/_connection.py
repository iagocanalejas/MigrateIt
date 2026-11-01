import importlib
import sys
from typing import TYPE_CHECKING, Any

from migrateit.models import SupportedDatabase
from migrateit.reporters.output import write_line_error

driver_configs = [
    ("sqlite3", "Error"),
    ("mysql.connector", "Error"),
    ("oracledb", "DatabaseError"),
    ("psycopg", "Error"),
]

exceptions = []
for module_name, error_attr in driver_configs:
    try:
        module = importlib.import_module(module_name)
        exceptions.append(getattr(module, error_attr))
    except ImportError, AttributeError:  # pragma: no cover
        pass

DatabaseError: tuple[type[Exception], ...] = tuple(exceptions)

if TYPE_CHECKING:
    import sqlite3

    import oracledb
    import psycopg
    from mysql.connector.abstracts import MySQLConnectionAbstract
    from mysql.connector.pooling import PooledMySQLConnection

    type _PsqlConnection = psycopg.Connection
    type _SqliteConnection = sqlite3.Connection
    type _MySqlConnection = MySQLConnectionAbstract | PooledMySQLConnection
    type _OracleConnection = oracledb.Connection
    type Connection = _PsqlConnection | _SqliteConnection | _MySqlConnection | _OracleConnection
else:
    # Fallback type at runtime so other modules can still import `Connection` safely
    type _PsqlConnection = Any
    type _SqliteConnection = Any
    type _MySqlConnection = Any
    type _OracleConnection = Any
    type Connection = Any


def get_connection(database: SupportedDatabase) -> Connection:
    if database is SupportedDatabase.POSTGRES:
        import psycopg

        from migrateit.clients.psql import PsqlClient

        try:
            return PsqlClient.get_connection()
        except psycopg.Error as e:
            write_line_error(f"Error connecting to PostgreSQL: {e}")
            sys.exit(1)
    if database is SupportedDatabase.SQLITE:
        import sqlite3

        from migrateit.clients.sqlite import SqliteClient

        try:
            return SqliteClient.get_connection()
        except sqlite3.Error as e:
            write_line_error(f"Error connecting to SQLite: {e}")
            sys.exit(1)
    if database is SupportedDatabase.MYSQL or database is SupportedDatabase.MARIADB:
        import mysql.connector

        from migrateit.clients.mysql import MySqlClient

        try:
            return MySqlClient.get_connection()
        except mysql.connector.Error as e:
            write_line_error(f"Error connecting to MySQL/MariaDB: {e}")
            sys.exit(1)
    if database is SupportedDatabase.ORACLE:
        import oracledb

        from migrateit.clients.oracle import OracleClient

        try:
            return OracleClient.get_connection()
        except oracledb.Error as e:
            write_line_error(f"Error connecting to Oracle: {e}")
            sys.exit(1)
    raise NotImplementedError(f"Database {database} is not supported")
