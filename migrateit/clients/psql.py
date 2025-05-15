import hashlib
import os
from pathlib import Path
from typing import override

from psycopg2 import DatabaseError, ProgrammingError, sql
from psycopg2.extensions import connection as Connection

from migrateit.clients._client import SqlClient
from migrateit.models import Migration, MigrationsFile, MigrationStatus


class PsqlClient(SqlClient[Connection]):
    @override
    @classmethod
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
    def retrieve_migrations(self, changelog: MigrationsFile) -> list[tuple[Migration, MigrationStatus]]:
        assert self.is_migrations_table_created(), f"Migrations table={self.table_name} does not exist"

        migrations = self._retrieve_applied_migrations(changelog)

        # add migrations that are in the changelog but not in the database
        for i, migration in enumerate(changelog.migrations):
            if not any(m[0].name == migration.name for m in migrations):
                migrations.insert(i, (migration, MigrationStatus.NOT_APPLIED))

        self._verify_migrations(changelog, migrations)
        return migrations

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
    def apply_migration(self, changelog: MigrationsFile, migration: Migration) -> None:
        path = self.migrations_dir / migration.name
        assert path.exists(), f"Migration file {path.name} does not exist"
        assert path.is_file(), f"Migration file {path.name} is not a file"
        assert any(m.name == migration.name for m in changelog.migrations), (
            f"Migration {migration.name} is not in the changelog"
        )
        assert path.name.endswith(".sql"), f"Migration file {path.name} must be a SQL file"
        assert not self.is_migration_applied(migration), f"Migration {path.name} has already been applied"

        content, migration_hash = self._get_content_hash(path)
        assert content, f"Migration file {path.name} is empty"

        _ = self.retrieve_migrations(changelog)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(content)
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

    def _retrieve_applied_migrations(self, changelog: MigrationsFile) -> list[tuple[Migration, MigrationStatus]]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("""SELECT migration_name, change_hash FROM {}""").format(sql.Identifier(self.table_name))
            )
            rows = cursor.fetchall()

        migrations = []
        for row in rows:
            migration_name, change_hash = row
            migration = next((m for m in changelog.migrations if m.name == migration_name), None)
            if migration:
                _, migration_hash = self._get_content_hash(self.migrations_dir / migration.name)
                status = MigrationStatus.APPLIED if migration_hash == change_hash else MigrationStatus.CONFLICT
                # migration applied or conflict
                migrations.append((migration, status))
            else:
                # migration applied not in changelog
                migrations.append((Migration(name=migration_name), MigrationStatus.REMOVED))
        return migrations

    def _verify_migrations(
        self, changelog: MigrationsFile, migrations: list[tuple[Migration, MigrationStatus]]
    ) -> None:
        first_not_applied = None
        for i, (m, s) in enumerate(migrations):
            if s == MigrationStatus.REMOVED:
                continue

            if first_not_applied is None and s == MigrationStatus.NOT_APPLIED:
                first_not_applied = i

            if m.name != changelog.migrations[i].name:
                raise ValueError(
                    f"Migration {m.name} is not in the same order as in the changelog. "
                    f"Expected {changelog.migrations[i].name} at index {i}"
                )

            if first_not_applied is not None and s != MigrationStatus.NOT_APPLIED:
                raise ValueError(
                    f"NOT_APPLIED migrations must be at the end of the list. Expected NOT_APPLIED at index {i}"
                )

    def _get_content_hash(self, path: Path) -> tuple[str, str]:
        content = path.read_text()
        return content, hashlib.sha256(content.encode("utf-8")).hexdigest()
