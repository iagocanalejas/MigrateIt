import sqlite3

import psycopg

from migrateit.clients.psql import PsqlClient
from migrateit.clients.sqlite import SqliteClient
from migrateit.models.changelog import SupportedDatabase

type Connection = psycopg.Connection | sqlite3.Connection


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
        case _:
            raise NotImplementedError(f"Database {database} is not supported")
