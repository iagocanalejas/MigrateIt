import hashlib
import os
import sqlite3
from pathlib import Path
from typing import override

from migrateit.clients._client import SqlClient
from migrateit.models import Migration, MigrationStatus
from migrateit.reporters.logs import logger
from migrateit.tree import ROLLBACK_SPLIT_TAG, build_migrations_tree


class SqliteClient(SqlClient[sqlite3.Connection]):
    @override
    @classmethod
    def get_environment_url(cls) -> str:
        """Get the SQLite database path from environment or use default."""
        db_url = os.getenv(cls.VARNAME_DB_URL)
        if db_url:
            return db_url
        db_file = os.getenv(cls.VARNAME_DB_FILE, "migrateit.db")
        return f"sqlite:///{db_file}"

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
        cursor = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
            (self.table_name,),
        )
        return cursor.fetchone() is not None

    @override
    def is_migration_applied(self, migration: Migration) -> bool:
        """Check if a migration has been applied in SQLite."""
        cursor = self.connection.execute(
            f"SELECT EXISTS(SELECT 1 FROM {self.table_name} WHERE migration_name=?);",
            (migration.name,),
        )
        result = cursor.fetchone()
        return bool(result[0]) if result else False

    @override
    def retrieve_migration_statuses(self) -> dict[str, MigrationStatus]:
        """Retrieve migration statuses from the SQLite database."""

        migrations = {k: MigrationStatus.NOT_APPLIED for k, _ in build_migrations_tree(self.changelog).items()}

        if not self.is_migrations_table_created():
            return migrations

        cursor = self.connection.execute(f"SELECT migration_name, change_hash FROM {self.table_name};")
        rows = cursor.fetchall()

        for row in rows:
            migration_name = row[0]
            change_hash = row[1]
            migration = next((m for m in self.changelog.migrations if m.name == migration_name), None)
            if not migration:
                migrations[migration_name] = MigrationStatus.REMOVED
                continue

            _, _, migration_hash = self._get_migration_content_and_hash(self.migrations_dir / migration.name)
            status = MigrationStatus.APPLIED
            if migration_hash != change_hash:
                status = MigrationStatus.CONFLICT
                logger.warning("Hash mismatch for %s: file=%s db=%s", migration_name, migration_hash, change_hash)

            migrations[migration.name] = status

        return migrations

    @override
    def apply_migration(self, migration: Migration, is_fake: bool = False, is_rollback: bool = False) -> None:
        """Apply a migration or rollback to SQLite."""
        path = self._get_migration_path(migration)
        if not migration.initial and not (self.is_migration_applied(migration) == is_rollback):
            if is_rollback:
                raise ValueError(f"Migration {path.name} is not applied, cannot undo it")
            raise ValueError(f"Migration {path.name} is already applied, cannot apply it again")

        migration_code, reverse_migration_code, migration_hash = self._get_migration_content_and_hash(path)

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
        with self.connection:
            self.connection.execute(
                f"UPDATE {self.table_name} SET squashed=1 WHERE migration_name IN ({placeholders});",
                migrations,
            )
            # Insert the new squashed migration record
            # Use empty content hash since the file may not exist yet
            hash = hashlib.sha256(b"").hexdigest()
            self.connection.execute(
                f"INSERT INTO {self.table_name} (migration_name, change_hash) VALUES (?, ?);",
                (os.path.basename(self.migrations_dir / new_migration.name), hash),
            )

    @override
    def update_migration_hash(self, migration: Migration) -> None:
        """Update the hash of a migration in SQLite."""
        path = self._get_migration_path(migration)
        _, _, migration_hash = self._get_migration_content_and_hash(path)

        self.connection.execute(
            f"INSERT OR REPLACE INTO {self.table_name} (migration_name, change_hash) VALUES (?, ?);",
            (os.path.basename(path), migration_hash),
        )

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
            for conflict_migration in conflict_migrations:
                path = self.migrations_dir / conflict_migration
                _, _, migration_hash = self._get_migration_content_and_hash(path)
                raise ValueError(
                    f"Migration {conflict_migration} has a different hash in the database: "
                    f"found={migration_hash} existing={self._get_database_hash(conflict_migration)}"
                )

        for migration in self.changelog.migrations:
            if status_map[migration.name] != MigrationStatus.APPLIED:
                continue
            for parent in migration.parents:
                if status_map[parent] != MigrationStatus.APPLIED:
                    raise ValueError(f"Migration {migration.name} is applied before its parent {parent}.")

    @override
    def validate_sql_syntax(self, migration: Migration) -> tuple[BaseException, str] | None:
        """Validate SQL syntax for SQLite by executing in a separate in-memory DB."""
        path = self._get_migration_path(migration)
        migration_code, reverse_migration_code, _ = self._get_migration_content_and_hash(path)

        try:
            memory_conn = sqlite3.connect(":memory:")
            try:
                for code in (migration_code, reverse_migration_code):
                    patched = self._patch_sql_for_validation(code)
                    if not patched:
                        continue
                    memory_conn.executescript(patched)
            finally:
                memory_conn.close()
        except sqlite3.Error as e:
            sql_for_error = (
                migration_code if "Rollback" not in (reverse_migration_code or "") else reverse_migration_code
            )
            return e, sql_for_error
        return None

    def _patch_sql_for_validation(self, sql: str) -> str:
        """Patch SQL for SQLite validation compatibility."""
        upper_sql = sql.upper()
        if "CREATE TABLE" in upper_sql and "IF NOT EXISTS" not in upper_sql:
            return sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
        if "DROP TABLE" in upper_sql and "DROP TABLE IF EXISTS" not in upper_sql:
            return sql.replace("DROP TABLE", "DROP TABLE IF EXISTS", 1)
        return sql

    def _update_migration_changelog(self, migration: Migration, hash: str, is_rollback: bool) -> None:
        """Insert or delete a migration record in SQLite."""
        if is_rollback and not migration.initial:
            self.connection.execute(
                f"DELETE FROM {self.table_name} WHERE migration_name=? AND change_hash=?;",
                (os.path.basename(self.migrations_dir / migration.name), hash),
            )
        else:
            self.connection.execute(
                f"INSERT INTO {self.table_name} (migration_name, change_hash) VALUES (?, ?);",
                (os.path.basename(self.migrations_dir / migration.name), hash),
            )

    def _get_migration_path(self, migration: Migration) -> Path:
        """Resolve the migration file path."""
        path = self.migrations_dir / migration.name
        if not path.is_file() or not path.name.endswith(".sql"):
            raise FileNotFoundError(f"Migration file {path.name} does not exist or is not a valid SQL file")
        return path

    def _get_migration_content_and_hash(self, path: Path) -> tuple[str, str, str]:
        """Read migration content and compute its SHA-256 hash."""
        content = path.read_text()
        migration, reverse_migration = content.split(ROLLBACK_SPLIT_TAG, 1)
        return (
            migration,
            reverse_migration,
            hashlib.sha256(content.encode("utf-8")).hexdigest(),
        )

    def _get_database_hash(self, migration_name: str) -> str:
        """Retrieve a migration's hash from the SQLite database."""
        cursor = self.connection.execute(
            f"SELECT change_hash FROM {self.table_name} WHERE migration_name=?;",
            (migration_name,),
        )
        result = cursor.fetchone()

        if not result or not result[0]:
            raise ValueError(f"Migration {migration_name} not found in the database")
        return result[0]
