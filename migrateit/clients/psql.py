import hashlib
import os
import re
from pathlib import Path
from typing import override

from psycopg2 import DatabaseError, ProgrammingError
from psycopg2.extensions import connection as Connection

from migrateit.clients._client import SqlClient
from migrateit.models import Migration, MigrationStatus
from migrateit.tree import ROLLBACK_SPLIT_TAG, build_migrations_tree


class PsqlClient(SqlClient[Connection]):
    @override
    @classmethod
    def get_environment_url(cls) -> str:
        db_url = os.getenv(cls.VARNAME_DB_URL)
        if not db_url:
            host = os.getenv(cls.VARNAME_DB_HOST, "localhost")
            port = os.getenv(cls.VARNAME_DB_PORT, "5432")
            user = os.getenv(cls.VARNAME_DB_USER, "postgres")
            password = os.getenv(cls.VARNAME_DB_PASS, "")
            db_name = os.getenv(cls.VARNAME_DB_NAME, "migrateit")
            db_url = f"postgresql://{user}{f':{password}' if password else ''}@{host}:{port}/{db_name}"
        if not db_url:
            raise ValueError("DB_URL environment variable is not set")
        return db_url

    @override
    @classmethod
    def create_migrations_table_str(cls, table_name: str) -> tuple[str, str]:
        if not table_name.isidentifier():
            raise ValueError(f"Unsafe table name: {table_name}")
        return (
            f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(255) UNIQUE NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_hash VARCHAR(64) NOT NULL
);
            """,
            f"""
DROP TABLE IF EXISTS {table_name};
            """,
        )

    @override
    def is_migrations_table_created(self) -> bool:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE LOWER(table_name) = LOWER('{self.table_name}')
                );
                """
            )
            result = cursor.fetchone()
            return result[0] if result else False

    @override
    def is_migration_applied(self, migration: Migration) -> bool:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""SELECT EXISTS (SELECT 1 FROM {self.table_name} WHERE migration_name = %s);""",
                (os.path.basename(migration.name),),
            )
            result = cursor.fetchone()
            return result[0] if result else False

    @override
    def retrieve_migration_statuses(self) -> dict[str, MigrationStatus]:
        migrations = {k: MigrationStatus.NOT_APPLIED for k, _ in build_migrations_tree(self.changelog).items()}

        if not self.is_migrations_table_created():
            return migrations

        with self.connection.cursor() as cursor:
            cursor.execute(f"""SELECT migration_name, change_hash FROM {self.table_name}""")
            rows = cursor.fetchall()

        for row in rows:
            migration_name, change_hash = row
            migration = next((m for m in self.changelog.migrations if m.name == migration_name), None)
            if not migration:
                # migration applied not in changelog
                migrations[migration_name] = MigrationStatus.REMOVED
                continue

            _, _, migration_hash = self._get_content_hash(self.migrations_dir / migration.name)
            status = MigrationStatus.APPLIED if migration_hash == change_hash else MigrationStatus.CONFLICT
            migrations[migration.name] = status

        return migrations

    @override
    def apply_migration(self, migration: Migration, is_fake: bool = False, is_rollback: bool = False) -> None:
        path = self.migrations_dir / migration.name
        if not path.is_file() or not path.name.endswith(".sql"):
            raise FileNotFoundError(f"Migration file {path.name} does not exist or is not a valid SQL file")
        if not migration.initial and not (self.is_migration_applied(migration) == is_rollback):
            if is_rollback:
                raise ValueError(f"Migration {path.name} is not applied, cannot undo it")
            raise ValueError(f"Migration {path.name} is already applied, cannot apply it again")

        migration_code, reverse_migration_code, migration_hash = self._get_content_hash(path)

        try:
            with self.connection.cursor() as cursor:
                if not is_fake:
                    cursor.execute(migration_code if not is_rollback else reverse_migration_code)
                    if migration.initial:
                        self.connection.commit()
                if is_rollback:
                    cursor.execute(
                        f"""DELETE FROM {self.table_name} where migration_name = %s and change_hash = %s;""",
                        (os.path.basename(path), migration_hash),
                    )
                else:
                    cursor.execute(
                        f"""INSERT INTO {self.table_name} (migration_name, change_hash) VALUES (%s, %s);""",
                        (os.path.basename(path), migration_hash),
                    )
        except (DatabaseError, ProgrammingError) as e:
            self.connection.rollback()
            raise e

    @override
    def update_migration_hash(self, migration: Migration) -> None:
        path = self.migrations_dir / migration.name
        if not path.is_file() or not path.name.endswith(".sql"):
            raise FileNotFoundError(f"Migration file {path.name} does not exist or is not a valid SQL file")

        _, _, migration_hash = self._get_content_hash(path)

        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""UPDATE {self.table_name} SET change_hash = %s WHERE migration_name = %s;""",
                (migration_hash, os.path.basename(path)),
            )
            self.connection.commit()

    @override
    def validate_migrations(self, status_map: dict[str, MigrationStatus]) -> None:
        if len(self.changelog.migrations) == 0:
            return

        if not self.changelog.migrations[0].initial:
            raise ValueError("Initial migration is not defined in the changelog")
        if len([m for m in self.changelog.migrations if m.initial]) > 1:
            raise ValueError("Multiple initial migrations found in the changelog")

        # check removed migrations
        removed_migrations = [m for m, s in status_map.items() if s == MigrationStatus.REMOVED]
        if removed_migrations:
            raise ValueError(f"Removed migrations found in the database: {removed_migrations}. ")

        # check conflict migrations
        conflict_migrations = [m for m, s in status_map.items() if s == MigrationStatus.CONFLICT]
        if conflict_migrations:
            for conflict_migration in conflict_migrations:
                path = self.migrations_dir / conflict_migration
                _, _, migration_hash = self._get_content_hash(path)
                raise ValueError(
                    f"Migration {conflict_migration} has a different hash in the database: "
                    f"found={migration_hash} existing={self._get_database_hash(conflict_migration)}"
                )

        # check for each migration all the parents are applied
        for migration in self.changelog.migrations:
            if status_map[migration.name] != MigrationStatus.APPLIED:
                continue
            for parent in migration.parents:
                if status_map[parent] != MigrationStatus.APPLIED:
                    raise ValueError(f"Migration {migration.name} is applied before its parent {parent}.")

    @override
    def validate_sql_syntax(self, migration: Migration) -> tuple[ProgrammingError, str] | None:
        path = self.migrations_dir / migration.name
        if not path.is_file() or not path.name.endswith(".sql"):
            raise FileNotFoundError(f"Migration file {path.name} does not exist or is not a valid SQL file")

        migration_code, reverse_migration_code, _ = self._get_content_hash(path)

        for code in (migration_code, reverse_migration_code):
            try:
                with self.connection.cursor() as cursor:
                    code = self._patch_sql_statement(code)
                    if not code:
                        continue
                    cursor.execute(code)
            except ProgrammingError as e:
                return e, code
            finally:
                self.connection.rollback()
        return None

    def _patch_sql_statement(self, sql: str) -> str:
        # remove comments
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        sql = re.sub(r"--.*(?=\n|$)", "", sql).strip()

        if not any(w in sql.upper() for w in ("CREATE", "ALTER", "DROP")):
            return sql
        if "CREATE TABLE" in sql.upper() and "IF NOT EXISTS" not in sql.upper():
            return sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
        if "DROP TABLE" in sql.upper() and "IF EXISTS" not in sql.upper():
            return sql.replace("DROP TABLE", "DROP TABLE IF EXISTS")
        if "ALTER TABLE" in sql.upper():
            if "ADD COLUMN" in sql.upper() and "IF NOT EXISTS" not in sql.upper():
                return sql.replace("ADD COLUMN", "ADD COLUMN IF NOT EXISTS")
            if "DROP COLUMN" in sql.upper() and "IF EXISTS" not in sql.upper():
                return sql.replace("DROP COLUMN", "DROP COLUMN IF EXISTS")
        return sql

    def _get_database_hash(self, migration_name: str) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""SELECT change_hash FROM {self.table_name} WHERE migration_name = %s""",
                (migration_name,),
            )
            result = cursor.fetchone()

            if not result or not result[0]:
                raise ValueError(f"Migration {migration_name} not found in the database")
            return result[0]

    def _get_content_hash(self, path: Path) -> tuple[str, str, str]:
        content = path.read_text()
        migration, reverse_migration = content.split(ROLLBACK_SPLIT_TAG, 1)
        return migration, reverse_migration, hashlib.sha256(content.encode("utf-8")).hexdigest()
