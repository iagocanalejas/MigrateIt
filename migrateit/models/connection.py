import sqlite3

import mysql.connector
import psycopg
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection

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
            params = PsqlClient.get_connection_params()
            pg_conn = psycopg.connect(**params, autocommit=False)
            return pg_conn
        case SupportedDatabase.SQLITE:
            params = SqliteClient.get_connection_params()
            file_name = params.get("file_name", params.get("url", "").replace("sqlite:///", ""))
            sqlite_conn = sqlite3.connect(file_name, autocommit=False)
            return sqlite_conn
        case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            params = MySqlClient.get_connection_params()
            if "connection_string" in params:
                # DB_URL was provided — use it directly
                mysql_conn = mysql.connector.connect(connection_string=params["connection_string"], autocommit=False)
            else:
                # Build kwargs without the password being in a URL string
                conn_kwargs = {
                    "host": params["host"],
                    "port": params["port"],
                    "user": params["user"],
                    "password": params["password"],
                    "database": params["database"],
                    "autocommit": False,
                }

                # Extract connection_timeout
                timeout_val = params["connection_timeout"]
                conn_kwargs["connection_timeout"] = int(timeout_val)

                mysql_conn = mysql.connector.connect(**conn_kwargs)
            return mysql_conn
        case _:
            raise NotImplementedError(f"Database {database} is not supported")
