import hashlib
import os
from pathlib import Path
from typing import override

from psycopg2 import DatabaseError, ProgrammingError, sql
from psycopg2.extensions import connection as Connection

from migrateit.clients._client import SqlClient
from migrateit.models import Migration, MigrationStatus


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
    def retrieve_migrations(self) -> dict[str, tuple[Migration, MigrationStatus]]:
        assert self.is_migrations_table_created(), f"Migrations table={self.table_name} does not exist"

        migrations = self._retrieve_applied_migrations()

        # add migrations that are in the changelog but not in the database
        for migration in self.changelog.migrations:
            if migration.name in migrations:
                continue
            migrations[migration.name] = (migration, MigrationStatus.NOT_APPLIED)

        self._verify_migrations(migrations)
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
    def apply_migration(self, migration: Migration, fake: bool) -> None:
        path = self.migrations_dir / migration.name
        assert path.exists(), f"Migration file {path.name} does not exist"
        assert path.is_file(), f"Migration file {path.name} is not a file"
        assert path.name.endswith(".sql"), f"Migration file {path.name} must be a SQL file"
        assert not self.is_migration_applied(migration), f"Migration {path.name} has already been applied"

        content, migration_hash = self._get_content_hash(path)
        assert content, f"Migration file {path.name} is empty"

        try:
            with self.connection.cursor() as cursor:
                if not fake:
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

    def _retrieve_applied_migrations(self) -> dict[str, tuple[Migration, MigrationStatus]]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("""SELECT migration_name, change_hash FROM {}""").format(sql.Identifier(self.table_name))
            )
            rows = cursor.fetchall()

        migrations = {}
        for row in rows:
            migration_name, change_hash = row
            migration = next((m for m in self.changelog.migrations if m.name == migration_name), None)
            if migration:
                _, migration_hash = self._get_content_hash(self.migrations_dir / migration.name)
                status = MigrationStatus.APPLIED if migration_hash == change_hash else MigrationStatus.CONFLICT
                # migration applied or conflict
                migrations[migration.name] = (migration, status)
            else:
                # migration applied not in changelog
                migrations[migration_name] = (Migration(name=migration_name), MigrationStatus.REMOVED)
        return migrations

    def _verify_migrations(self, migrations: dict[str, tuple[Migration, MigrationStatus]]) -> None:
        for m, s in migrations.values():
            if s == MigrationStatus.REMOVED:
                continue

            if s == MigrationStatus.APPLIED:
                parents = [migrations[p] for p in m.parents]
                assert all(s1 == MigrationStatus.APPLIED for _, s1 in parents), (
                    f"Migration {m.name} has parents that are not applied: {[p[0].name for p in parents]}"
                )

    def _get_content_hash(self, path: Path) -> tuple[str, str]:
        content = path.read_text()
        return content, hashlib.sha256(content.encode("utf-8")).hexdigest()
