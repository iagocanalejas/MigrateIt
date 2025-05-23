import hashlib
import os
from pathlib import Path
from typing import override

from psycopg2 import DatabaseError, ProgrammingError, sql
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
    def is_migrations_table_created(self) -> bool:
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
    def is_migration_applied(self, migration: Migration) -> bool:
        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM {}
                        WHERE migration_name = %s
                    );
                """).format(sql.Identifier(self.table_name)),
                (os.path.basename(migration.name),),
            )
            result = cursor.fetchone()
            return result[0] if result else False

    @override
    def create_migrations_table(self) -> None:
        assert not self.is_migrations_table_created(), f"Migrations table={self.table_name} already exists"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("""
                        CREATE TABLE {} (
                            id SERIAL PRIMARY KEY,
                            migration_name VARCHAR(255) UNIQUE NOT NULL,
                            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            change_hash VARCHAR(64) NOT NULL
                        );
                    """).format(sql.Identifier(self.table_name))
                )
                self.connection.commit()
        except (DatabaseError, ProgrammingError) as e:
            self.connection.rollback()
            raise e

    @override
    def retrieve_migration_statuses(self) -> dict[str, MigrationStatus]:
        assert self.is_migrations_table_created(), f"Migrations table={self.table_name} does not exist"

        migrations = {k: MigrationStatus.NOT_APPLIED for k, _ in build_migrations_tree(self.changelog).items()}

        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("""SELECT migration_name, change_hash FROM {}""").format(sql.Identifier(self.table_name))
            )
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
        assert path.exists(), f"Migration file {path.name} does not exist"
        assert path.is_file(), f"Migration file {path.name} is not a file"
        assert path.name.endswith(".sql"), f"Migration file {path.name} must be a SQL file"
        assert self.is_migration_applied(migration) == is_rollback, (
            f"Migration {path.name} is already applied, cannot apply it again"
            if not is_rollback
            else f"Migration {path.name} is not applied, cannot undo it"
        )

        migration_code, reverse_migration_code, migration_hash = self._get_content_hash(path)

        try:
            with self.connection.cursor() as cursor:
                if not is_fake:
                    cursor.execute(migration_code if not is_rollback else reverse_migration_code)
                if is_rollback:
                    cursor.execute(
                        sql.SQL("""
                            DELETE FROM {} where migration_name = %s and change_hash = %s;
                        """).format(sql.Identifier(self.table_name)),
                        (os.path.basename(path), migration_hash),
                    )
                else:
                    cursor.execute(
                        sql.SQL("""
                            INSERT INTO {} (migration_name, change_hash)
                            VALUES (%s, %s);
                        """).format(sql.Identifier(self.table_name)),
                        (os.path.basename(path), migration_hash),
                    )
        except (DatabaseError, ProgrammingError) as e:
            self.connection.rollback()
            raise e

    @override
    def update_migration_hash(self, migration: Migration) -> None:
        path = self.migrations_dir / migration.name
        assert path.exists(), f"Migration file {path.name} does not exist"
        assert path.is_file(), f"Migration file {path.name} is not a file"
        assert path.name.endswith(".sql"), f"Migration file {path.name} must be a SQL file"

        _, _, migration_hash = self._get_content_hash(path)

        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("""
                    UPDATE {} SET change_hash = %s WHERE migration_name = %s;
                """).format(sql.Identifier(self.table_name)),
                (migration_hash, os.path.basename(path)),
            )
            self.connection.commit()

    @override
    def validate_migrations(self, status_map: dict[str, MigrationStatus]) -> None:
        if len(self.changelog.migrations) == 0:
            return

        assert self.changelog.migrations[0].initial, "Initial migration not found in changelog"
        assert len([m for m in self.changelog.migrations if m.initial]) == 1, (
            "Multiple initial migrations found in changelog"
        )

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
    def validate_sql_sintax(self, migration: Migration) -> tuple[ProgrammingError, str] | None:
        path = self.migrations_dir / migration.name
        assert path.exists(), f"Migration file {path.name} does not exist"
        assert path.is_file(), f"Migration file {path.name} is not a file"
        assert path.name.endswith(".sql"), f"Migration file {path.name} must be a SQL file"

        migration_code, reverse_migration_code, _ = self._get_content_hash(path)
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(f"PREPARE stmt AS {migration_code}")
                cursor.execute("DEALLOCATE stmt")
            except ProgrammingError as e:
                return e, migration_code
            finally:
                self.connection.rollback()
            try:
                cursor.execute(f"PREPARE rev_stmt AS {reverse_migration_code}")
                cursor.execute("DEALLOCATE rev_stmt")
            except ProgrammingError as e:
                return e, reverse_migration_code
            finally:
                self.connection.rollback()
        return None

    def _get_database_hash(self, migration_name: str) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("""
                    SELECT change_hash FROM {} WHERE migration_name = %s
                """).format(sql.Identifier(self.table_name)),
                (migration_name,),
            )
            result = cursor.fetchone()
            assert result and result[0], f"Migration {migration_name} not found in the database"
            return result[0]

    def _get_content_hash(self, path: Path) -> tuple[str, str, str]:
        content = path.read_text()
        migration, reverse_migration = content.split(ROLLBACK_SPLIT_TAG, 1)
        return migration, reverse_migration, hashlib.sha256(content.encode("utf-8")).hexdigest()
