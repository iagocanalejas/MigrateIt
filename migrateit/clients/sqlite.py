import os
import sqlite3
from typing import override

from migrateit.clients._client import SqlClient
from migrateit.models.migration import Migration, MigrationStatus
from migrateit.reporters.logs import logger


class SqliteClient(SqlClient[sqlite3.Connection]):
    @override
    @classmethod
    def get_environment_url(cls) -> str:
        db_url = os.getenv(cls.VARNAME_DB_URL)
        if db_url:
            return db_url

        db_file = os.getenv(cls.VARNAME_DB_FILE, "migrateit.db")
        return f"sqlite:///{os.path.abspath(db_file)}"

    @override
    @classmethod
    def create_migrations_table_str(cls, table_name: str) -> tuple[str, str]:
        """Create SQLite DDL for the migrations table."""
        if not table_name.isidentifier():
            raise ValueError(f"Unsafe table name: {table_name}")
        migrations_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_name VARCHAR(255) UNIQUE NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_hash VARCHAR(64) NOT NULL,
    squashed BOOLEAN DEFAULT 0
);
        """
        reverse_query = f"""
DROP TABLE IF EXISTS {table_name};
        """
        return migrations_query, reverse_query

    @override
    def is_migrations_table_created(self) -> bool:
        """Check if the migrations table exists in SQLite."""
        query = """
SELECT name
FROM sqlite_master
WHERE type='table' AND name=?;
"""
        cursor = self.connection.execute(query, (self.table_name,))
        return cursor.fetchone() is not None

    @override
    def is_migration_applied(self, migration: Migration) -> bool:
        """Check if a migration has been applied in SQLite."""
        query = f"""
SELECT EXISTS(
    SELECT 1 FROM {self.table_name} WHERE migration_name=?
);
"""
        cursor = self.connection.execute(query, (migration.name,))
        result = cursor.fetchone()
        return bool(result[0]) if result else False

    @override
    def retrieve_migration_statuses(self) -> dict[str, MigrationStatus]:
        """Retrieve migration statuses from the SQLite database."""

        migrations = {k: MigrationStatus.NOT_APPLIED for k, _ in self.changelog.migrations_tree.items()}

        if not self.is_migrations_table_created():
            return migrations

        query = f"""
SELECT migration_name, change_hash
FROM {self.table_name};
"""
        cursor = self.connection.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            migration_name = row[0]
            change_hash = row[1]
            migration = next((m for m in self.changelog.migrations if m.name == migration_name), None)
            if not migration:
                migrations[migration_name] = MigrationStatus.REMOVED
                continue

            _, _, migration_hash = self.get_migration_content_and_hash(self.migrations_dir / migration.name)
            status = MigrationStatus.APPLIED
            if migration_hash != change_hash:
                status = MigrationStatus.CONFLICT
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
            if not is_fake:
                code = migration_code if not is_rollback else reverse_migration_code
                self.connection.executescript(code)
            self._update_migration_changelog(migration, migration_hash, is_rollback)
        except sqlite3.Error as e:
            self.connection.rollback()
            raise e

    @override
    def squash_migrations(self, migrations: list[str], new_migration: Migration) -> None:
        """Mark migrations as squashed in SQLite."""
        placeholders = ",".join("?" for _ in migrations)
        query = f"""
UPDATE {self.table_name} SET squashed=1
WHERE migration_name IN ({placeholders});
"""
        with self.connection:
            self.connection.execute(query, migrations)
        self.apply_migration(new_migration, is_fake=True)

    @override
    def update_migration_hash(self, migration: Migration) -> None:
        """Update the hash of a migration in SQLite."""
        path = self.get_migration_path(migration)
        _, _, migration_hash = self.get_migration_content_and_hash(path)

        query = f"""
INSERT OR REPLACE INTO {self.table_name} (migration_name, change_hash)
VALUES (?, ?);
"""
        cursor = self.connection.execute(query, (path.name, migration_hash))
        cursor.fetchall()

    @override
    def validate_migrations(self, status_map: dict[str, MigrationStatus]) -> None:
        """Validate migration statuses for SQLite."""
        if len(self.changelog.migrations) == 0:
            return

        if not self.changelog.migrations[0].initial:
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

    def _update_migration_changelog(self, migration: Migration, hash: str, is_rollback: bool) -> None:
        """Insert or delete a migration record in SQLite."""
        if is_rollback and not migration.initial:
            query = f"""
DELETE FROM {self.table_name}
WHERE migration_name=?
    AND change_hash=?;
"""
        else:
            query = f"""
INSERT INTO {self.table_name} (migration_name, change_hash)
VALUES (?, ?);
"""
        self.connection.execute(query, ((self.migrations_dir / migration.name).name, hash))

    def _get_database_hash(self, migration_name: str) -> str:
        """Retrieve a migration's hash from the SQLite database."""
        query = f"""
SELECT change_hash
FROM {self.table_name}
WHERE migration_name=?;
"""
        cursor = self.connection.execute(query, (migration_name,))
        result = cursor.fetchone()

        if not result or not result[0]:
            raise ValueError(f"Migration {migration_name} not found in the database")
        return result[0]
