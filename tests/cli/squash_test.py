import sqlite3
from pathlib import Path
from typing import Any

import pytest

from migrateit.cli import cmd_new, cmd_run, cmd_squash
from migrateit.clients import SqlClient
from tests.conftest import create_migration_file


@pytest.mark.integration
def test_cmd_squash_all_applied(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    cmd_new(cmd_client, name="first", no_edit=True)
    create_migration_file(cmd_client.migrations_dir, "0001_first.sql")
    cmd_new(cmd_client, name="second", no_edit=True)
    create_migration_file(cmd_client.migrations_dir, "0002_second.sql")

    cmd_run(cmd_client)  # apply both

    result = cmd_squash(cmd_client, start_migration="0001", end_migration="0002", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in cmd_client.changelog.migrations]
    assert "0003_squashed_0001_0002.sql" in changelog_names
    assert "0001_first" not in changelog_names
    assert "0002_second" not in changelog_names

    # Verify squashed state in database
    if isinstance(cmd_client.connection, sqlite3.Connection):
        cursor = cmd_client.connection.execute("SELECT migration_name, squashed FROM migrations")
        applied = {row[0]: row[1] for row in cursor.fetchall()}
    else:
        with cmd_client.connection.cursor() as cursor:
            cursor.execute("SELECT migration_name, squashed FROM migrations")
            applied = {row[0]: row[1] for row in cursor.fetchall()}

    assert "0003_squashed_0001_0002.sql" in applied
    assert applied["0001_first.sql"] == 1
    assert applied["0002_second.sql"] == 1


@pytest.mark.integration
def test_cmd_squash_not_all_applied(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    cmd_new(cmd_client, name="first", no_edit=True)
    create_migration_file(cmd_client.migrations_dir, "0001_first.sql")

    cmd_new(cmd_client, name="second", no_edit=True)
    create_migration_file(cmd_client.migrations_dir, "0002_second.sql")

    result = cmd_squash(cmd_client, start_migration="0001", end_migration="0002", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in cmd_client.changelog.migrations]
    assert "0003_squashed_0001_0002.sql" in changelog_names
    assert "0001_first" not in changelog_names
    assert "0002_second" not in changelog_names


def test_cmd_squash_no_end_too_few_migrations(cmd_client: SqlClient[Any]) -> None:
    """Test cmd_squash raises ValueError when end_migration is None and fewer than 3 migrations."""
    with pytest.raises(ValueError, match="squash less than 3"):
        cmd_squash(cmd_client, start_migration="0000_migrateit.sql")


@pytest.mark.integration
def test_cmd_squash_no_end_uses_last_migration(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test cmd_squash uses the last migration as end_migration when not provided."""
    cmd_new(cmd_client, name="first", no_edit=True)
    create_migration_file(cmd_client.migrations_dir, "0001_first.sql")

    cmd_new(cmd_client, name="second", no_edit=True)
    create_migration_file(cmd_client.migrations_dir, "0002_second.sql")

    result = cmd_squash(cmd_client, start_migration="0001", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in cmd_client.changelog.migrations]
    assert "0003_squashed_0001_0002.sql" in changelog_names
    assert "0001_first" not in changelog_names
    assert "0002_second" not in changelog_names
