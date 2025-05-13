from typing import Protocol

from migrateit.models import Migration, MigrationsFile, MigrationStatus


class SqlClientProtocol(Protocol):
    @classmethod
    def get_environment_url(cls) -> str:
        """
        Get the database URL from the environment variables.

        Returns:
            The database URL as a string.
        """
        ...

    def is_migrations_table_created(self) -> bool:
        """
        Check if the migrations table exists in the database.

        Returns:
            True if the table exists, False otherwise.
        """
        ...

    def create_migrations_table(self) -> None:
        """
        Create the migrations table in the database.
        """
        ...

    def retrieve_migrations(self, changelog: MigrationsFile) -> list[tuple[Migration, MigrationStatus]]:
        """
        Validate the changelog file.

        Args:
            changelog: The changelog object to validate.

        Returns:
            A list of tuples containing the migration object and a boolean indicating if it has been applied.
        """
        ...

    def is_migration_applied(self, migration: Migration) -> bool:
        """
        Check if a migration has already been applied.

        Args:
            migration: The migration object to check.

        Returns:
            True if the migration has been applied, False otherwise.
        """
        ...

    def apply_migration(self, changelog: MigrationsFile, migration: Migration) -> None:
        """
        Apply a migration to the database.

        Args:
            changelog: The changelog object containing the migration.
            migration: The migration object to apply.
        """
        ...
