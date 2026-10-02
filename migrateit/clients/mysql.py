import os
from typing import Any, override

import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.models.migration import Migration, MigrationStatus
from migrateit.reporters.logs import logger
from migrateit.reporters.output import write_line


def _q(name: str) -> str:
    """Wrap a table/column name in MySQL backticks (safe: validated by isidentifier)."""
    return f"`{name}`"


class MySqlClient(SqlClient[MySQLConnectionAbstract | PooledMySQLConnection]):
    @override
    @classmethod
    def get_environment_url(cls) -> str:
        db_url = os.getenv(cls.VARNAME_DB_URL)
        if db_url:
            return db_url

        host = os.getenv(cls.VARNAME_DB_HOST, "localhost")
        port = os.getenv(cls.VARNAME_DB_PORT, "3306")
        user = os.getenv(cls.VARNAME_DB_USER, "root")
        password = os.getenv(cls.VARNAME_DB_PASS, "")
        db_name = os.getenv(cls.VARNAME_DB_NAME, "migrateit")
        db_timeout = os.getenv(cls.VARNAME_DB_TIMEOUT_SECONDS, C.DEFAULT_TIMEOUT_SECONDS)

        password = f":{password}" if password else ""
        db_url = f"mysql://{user}{password}@{host}:{port}/{db_name}?connect_timeout={db_timeout}"
        return db_url

    @override
    @classmethod
    def create_migrations_table_str(cls, table_name: str) -> tuple[str, str]:
        if not table_name.isidentifier():
            raise ValueError(f"Unsafe table name: {table_name}")
        migrations_query = f"""
CREATE TABLE IF NOT EXISTS {_q(table_name)} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    migration_name VARCHAR(255) UNIQUE NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_hash VARCHAR(64) NOT NULL,
    squashed TINYINT(1) DEFAULT 0
);
        """
        reverse_query = f"""
DROP TABLE IF EXISTS {_q(table_name)};
        """
        return migrations_query, reverse_query

    @override
    def is_migrations_table_created(self) -> bool:
        with self.connection.cursor() as cursor:
            query = """
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
        AND LOWER(table_name) = LOWER(%s)
);
            """
            cursor.execute(query, (self.table_name,))
            result = cursor.fetchone()
            return bool(result[0]) if result else False  # type: ignore

    @override
    def is_migration_applied(self, migration: Migration) -> bool:
        query = f"""
SELECT EXISTS(
    SELECT 1 FROM {_q(self.table_name)} WHERE migration_name = %s
);
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query, (migration.name,))
            result = cursor.fetchone()
            return bool(result[0]) if result else False  # type: ignore

    @override
    def retrieve_migration_statuses(self) -> dict[str, MigrationStatus]:
        migrations = {k: MigrationStatus.NOT_APPLIED for k, _ in self.changelog.migrations_tree.items()}

        if not self.is_migrations_table_created():
            return migrations

        query = f"""
SELECT migration_name, change_hash
FROM {_q(self.table_name)};
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        for row in rows:
            migration_name: str = row[0]  # type: ignore
            change_hash: str = row[1]  # type: ignore
            migration = next((m for m in self.changelog.migrations if m.name == migration_name), None)
            if not migration:
                migrations[migration_name] = MigrationStatus.REMOVED
                continue

            _, _, migration_hash = self.get_migration_content_and_hash(self.migrations_dir / migration.name)
            status = MigrationStatus.APPLIED
            if migration_hash != change_hash:
                status = MigrationStatus.CONFLICT
                write_line(f"Hash mismatch for {migration_name}: file={migration_hash} db={change_hash}")
                logger.warning("Hash mismatch for %s: file=%s db=%s", migration_name, migration_hash, change_hash)

            migrations[migration.name] = status

        return migrations

    @override
    def apply_migration(self, migration: Migration, is_fake: bool = False, is_rollback: bool = False) -> None:
        if is_fake and is_rollback:
            raise ValueError("Cannot fake a rollback migration")

        path = self.get_migration_path(migration)
        if not migration.initial and not (self.is_migration_applied(migration) == is_rollback):
            if is_rollback:
                raise ValueError(f"Migration {path.name} is not applied, cannot undo it")
            raise ValueError(f"Migration {path.name} is already applied, cannot apply it again")

        migration_code, reverse_migration_code, migration_hash = self.get_migration_content_and_hash(path)

        try:
            with self.connection.cursor() as cursor:
                if not is_fake:
                    code = migration_code if not is_rollback else reverse_migration_code
                    cursor.execute(code)
                    cursor.fetchall()
                self._update_migration_changelog(cursor, migration, migration_hash, is_rollback)
        except mysql.connector.Error as e:
            self.connection.rollback()
            raise e

    @override
    def squash_migrations(self, migrations: list[str], new_migration: Migration) -> None:
        placeholders = ", ".join(["%s"] * len(migrations))
        query = f"""
UPDATE {_q(self.table_name)}
SET squashed=1
WHERE migration_name IN ({placeholders});
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query, migrations)
            cursor.fetchall()
        self.apply_migration(new_migration, is_fake=True)

    @override
    def update_migration_hash(self, migration: Migration) -> None:
        path = self.get_migration_path(migration)
        _, _, migration_hash = self.get_migration_content_and_hash(path)

        query = f"""
UPDATE {_q(self.table_name)}
SET change_hash = %s
WHERE migration_name = %s;
"""
        with self.connection.cursor() as cursor:
            cursor.execute(query, (migration_hash, path.name))
            cursor.fetchall()

    @override
    def validate_migrations(self, status_map: dict[str, MigrationStatus]) -> None:
        if len(self.changelog.migrations) == 0:
            return

        if not self.changelog.root.initial:
            raise ValueError("Initial migration is not defined in the changelog")
        if len([m for m in self.changelog.migrations if m.initial]) > 1:
            raise ValueError("Multiple initial migrations found in the changelog")

        removed_migrations = [m for m, s in status_map.items() if s == MigrationStatus.REMOVED]
        if removed_migrations:
            raise ValueError(f"Removed migrations found in the database: {removed_migrations}. ")

        conflict_migrations = [m for m, s in status_map.items() if s == MigrationStatus.CONFLICT]
        if conflict_migrations:
            errors: list[str] = []
            for conflict_migration in conflict_migrations:
                path = self.migrations_dir / conflict_migration
                _, _, migration_hash = self.get_migration_content_and_hash(path)
                errors.append(
                    f"Migration {conflict_migration} has a different hash in the database: "
                    f"found={migration_hash} existing={self._get_database_hash(conflict_migration)}"
                )
            raise ValueError("\n".join(errors))

        for migration in self.changelog.migrations:
            if status_map[migration.name] != MigrationStatus.APPLIED:
                continue
            for parent in migration.parents:
                if status_map[parent] != MigrationStatus.APPLIED:
                    raise ValueError(f"Migration {migration.name} is applied before its parent {parent}.")

    @override
    def _patch_sql_statement(self, sql: str) -> str:
        sql = super()._patch_sql_statement(sql)
        if not any(w in sql for w in ("CREATE ", "ALTER ", "DROP ")):
            return sql
        if "ALTER TABLE" in sql:
            if "ADD COLUMN" in sql and "IF NOT EXISTS" not in sql:
                return sql.replace("ADD COLUMN", "ADD COLUMN IF NOT EXISTS", 1)
        return sql

    def _update_migration_changelog(
        self,
        cursor: Any,
        migration: Migration,
        hash: str,
        is_rollback: bool,
    ) -> None:
        path = self.migrations_dir / migration.name
        if is_rollback and not migration.initial:
            query = f"""
DELETE FROM {_q(self.table_name)}
WHERE migration_name = %s
    AND change_hash = %s;
"""
        else:
            query = f"""
INSERT INTO {_q(self.table_name)} (migration_name, change_hash)
VALUES (%s, %s);
"""
        cursor.execute(query, (path.name, hash))
        cursor.fetchall()

    def _get_database_hash(self, migration_name: str) -> str:
        with self.connection.cursor() as cursor:
            query = f"""
SELECT change_hash
FROM {_q(self.table_name)}
WHERE migration_name = %s;
"""
            cursor.execute(query, (migration_name,))
            result = cursor.fetchone()

            if not result or not result[0]:  # type: ignore
                raise ValueError(f"Migration {migration_name} not found in the database")
            return str(result[0])  # type: ignore
