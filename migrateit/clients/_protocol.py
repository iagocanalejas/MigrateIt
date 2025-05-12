from typing import Protocol


class SqlClientProtocol(Protocol):
    @classmethod
    def get_environment_url(cls) -> str:
        """
        Get the database URL from the environment variables.

        Returns:
            The database URL as a string.
        """
        ...

    def check_migrations_table_exist(self) -> bool:
        """
        Check if the migrations table exists in the database.

        Args:
            conn: The database connection object.
            table_name: The name of the migrations table.

        Returns:
            True if the table exists, False otherwise.
        """
        ...

    def create_migrations_table(self) -> None:
        """
        Create the migrations table in the database.

        Args:
            conn: The database connection object.
            table_name: The name of the migrations table.
        """
        ...
