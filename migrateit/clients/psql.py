import os
from typing import override

from psycopg2 import DatabaseError, ProgrammingError, sql
from psycopg2.extensions import connection as Connection

from migrateit.clients._client import SqlClient


class PsqlClient(SqlClient[Connection]):
    @classmethod
    @override
    def get_environment_url(cls) -> str:
        db_url = os.getenv("DB_URL")
        if not db_url:
            host = os.getenv("DB_HOST", "localhost")
            port = os.getenv("DB_PORT", "5432")
            user = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASS", "")
            db_name = os.getenv("DB_NAME", "migrateit")
            db_url = f"postgresql://{user}{f':{password}' if password else ''}@{host}:{port}/{db_name}"
        if not db_url:
            raise ValueError("DB_URL environment variable is not set")
        return db_url

    @override
    def check_migrations_table_exist(self) -> bool:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE LOWER(table_name) = LOWER(%s)
                );
                """,
                (self.table_name,),
            )
            result = cursor.fetchone()
            return result[0] if result else False

    @override
    def create_migrations_table(self) -> None:
        assert not self.check_migrations_table_exist(), f"Migrations table={self.table_name} already exists"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("""
                        CREATE TABLE {} (
                            id SERIAL PRIMARY KEY,
                            migration_name VARCHAR(255) NOT NULL,
                            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            change_hash VARCHAR(64) NOT NULL
                        );
                    """).format(sql.Identifier(self.table_name))
                )
                self.connection.commit()
        except (DatabaseError, ProgrammingError) as e:
            self.connection.rollback()
            raise e
