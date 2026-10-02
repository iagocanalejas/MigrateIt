import sqlite3
from urllib.parse import parse_qs, urlparse

import mysql.connector
import psycopg
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection

from migrateit import constants as C
from migrateit.clients.mysql import MySqlClient
from migrateit.clients.psql import PsqlClient
from migrateit.clients.sqlite import SqliteClient
from migrateit.models.changelog import SupportedDatabase

type _PsqlConnection = psycopg.Connection
type _SqliteConnection = sqlite3.Connection
type _MySqlConnection = MySQLConnectionAbstract | PooledMySQLConnection
type Connection = _PsqlConnection | _SqliteConnection | _MySqlConnection


def get_connection(database: SupportedDatabase) -> Connection:
    match database:
        case SupportedDatabase.POSTGRES:
            pg_conn = psycopg.connect(PsqlClient.get_environment_url())
            pg_conn.autocommit = False
            return pg_conn
        case SupportedDatabase.SQLITE:
            db_url = SqliteClient.get_environment_url()
            sqlite_conn = sqlite3.connect(db_url.replace("sqlite:///", ""))
            sqlite_conn.autocommit = False
            return sqlite_conn
        case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            db_url = MySqlClient.get_environment_url()
            parsed = urlparse(db_url)
            query_params = parse_qs(parsed.query)

            # Base connection dictionary
            conn_kwargs = {
                "host": parsed.hostname,
                "port": parsed.port,
                "user": parsed.username,
                "password": parsed.password,
                "database": parsed.path.lstrip("/"),
            }

            # Extract connection_timeout (or timeout) if present in the URL
            timeout_val = query_params.get(
                "connection_timeout",
                query_params.get("timeout", [f"{C.DEFAULT_TIMEOUT_SECONDS}"]),
            )
            conn_kwargs["connection_timeout"] = int(timeout_val[0])

            mysql_conn = mysql.connector.connect(**conn_kwargs)
            setattr(mysql_conn, "autocommit", False)
            return mysql_conn
        case _:
            raise NotImplementedError(f"Database {database} is not supported")
