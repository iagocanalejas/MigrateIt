from typing import Protocol

from psycopg2 import ProgrammingError

from migrateit.models import Migration, MigrationStatus


class SqlClientProtocol(Protocol):
    @classmethod
    def get_environment_url(cls) -> str:
        """
        Get the database URL from the environment variables.

        Returns:
            The database URL as a string.
        """
        ...

    @classmethod
    def create_migrations_table_str(cls, table_name: str) -> tuple[str, str]:
        """
        Create the SQL string to create the migrations table.

        Args:
            table_name: The name of the migrations table.

        Returns:
            The SQL string to create the migrations table and it's rollback.
        """
        ...

    def is_migrations_table_created(self) -> bool:
        """
        Check if the migrations table exists in the database.

        Returns:
            True if the table exists, False otherwise.
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

    def retrieve_migration_statuses(self) -> dict[str, MigrationStatus]:
        """
        Retrieve the migrations from the database.
        Returns:
            A dictionary mapping migration names to their statuses.
        """
        ...

    def apply_migration(self, migration: Migration, is_fake: bool = False, is_rollback: bool = False) -> None:
        """
        Apply a migration to the database.

        Args:
            migration: The migration object to apply.
            fake: If True, apply the migration without executing it.
            undo: If True, apply the reverse of the migration.
        """
        ...

    def update_migration_hash(self, migration: Migration) -> None:
        """
        Update the hash of a migration in the database.

        Args:
            migration: The migration object to update.
        """
        ...

    def validate_migrations(self, status_map: dict[str, MigrationStatus]) -> None:
        """
        Validate the migrations in the database.

        Args:
            status_map: A dictionary mapping migration names to their statuses.

        Raises:
            ValueError: If there are any inconsistencies in the migration statuses.
        """
        ...

    def validate_sql_syntax(self, migration: Migration) -> tuple[ProgrammingError, str] | None:
        """
        Validate the SQL syntax of a migration.

        Args:
            migration: The migration object to validate.

        Returns:
            A tuple containing the error and the SQL query if there is a syntax error, None otherwise.
        """
        ...
