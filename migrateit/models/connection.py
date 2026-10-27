from migrateit.clients.mysql import MySqlClient
from migrateit.clients.oracle import OracleClient
from migrateit.clients.psql import PsqlClient
from migrateit.clients.sqlite import SqliteClient
from migrateit.models.config import SupportedDatabase
from migrateit.models.types import Connection


def get_connection(database: SupportedDatabase) -> Connection:
    if database is SupportedDatabase.POSTGRES:
        import psycopg

        params = PsqlClient.get_connection_params()
        pg_conn = psycopg.connect(**params, autocommit=False)
        return pg_conn
    if database is SupportedDatabase.SQLITE:
        import sqlite3

        params = SqliteClient.get_connection_params()
        file_name = params.get("file_name") or params.get("url", "").replace("sqlite:///", "")
        sqlite_conn = sqlite3.connect(file_name, autocommit=False)
        return sqlite_conn
    if database is SupportedDatabase.MYSQL or database is SupportedDatabase.MARIADB:
        import mysql.connector

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
    if database is SupportedDatabase.ORACLE:
        import oracledb

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
    raise NotImplementedError(f"Database {database} is not supported")
