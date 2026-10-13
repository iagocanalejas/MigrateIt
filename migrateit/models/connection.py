from __future__ import annotations

import sqlite3

import mysql.connector
import oracledb
import psycopg
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection

from migrateit.clients.mysql import MySqlClient
from migrateit.clients.oracle import OracleClient
from migrateit.clients.psql import PsqlClient
from migrateit.clients.sqlite import SqliteClient
from migrateit.models.config import SupportedDatabase

type _PsqlConnection = psycopg.Connection
type _SqliteConnection = sqlite3.Connection
type _MySqlConnection = MySQLConnectionAbstract | PooledMySQLConnection
type _OracleConnection = oracledb.Connection
type Connection = _PsqlConnection | _SqliteConnection | _MySqlConnection | _OracleConnection


def get_connection(database: SupportedDatabase) -> Connection:
    match database:
        case SupportedDatabase.POSTGRES:
            params = PsqlClient.get_connection_params()
            pg_conn = psycopg.connect(**params, autocommit=False)
            return pg_conn
        case SupportedDatabase.SQLITE:
            params = SqliteClient.get_connection_params()
            file_name = params.get("file_name") or params.get("url", "").replace("sqlite:///", "")
            sqlite_conn = sqlite3.connect(file_name, autocommit=False)
            return sqlite_conn
        case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            params = MySqlClient.get_connection_params()
            if "connection_string" in params:
                return mysql.connector.connect(connection_string=params["connection_string"], autocommit=False)
            conn_kwargs = {
                "host": params["host"],
                "port": params["port"],
                "user": params["user"],
                "password": params["password"],
                "database": params["database"],
                "autocommit": False,
                "connection_timeout": int(params["connection_timeout"]) if "connection_timeout" in params else None,
            }
            return mysql.connector.connect(**conn_kwargs)
        case SupportedDatabase.ORACLE:
            params = OracleClient.get_connection_params()
            if "dsn" in params:
                return oracledb.connect(dsn=params["dsn"], autocommit=False)
            conn_kwargs = {
                "user": params["user"],
                "password": params["password"],
                "host": params.get("host", "localhost"),
                "port": params.get("port", 1521),
                "service_name": params.get("service_name"),
                "sid": params.get("sid"),
            }
            return oracledb.connect(**conn_kwargs)
        case _:
            raise NotImplementedError(f"Database {database} is not supported")
