import sqlite3
from pathlib import Path
from typing import Any

import pytest

from migrateit.cli import cmd_new, cmd_run, cmd_squash
from migrateit.clients import SqlClient
from tests.conftest import create_migration_file


@pytest.mark.integration
def test_cmd_squash_applied(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test squashing applied migrations across databases."""
    client = cmd_client

    cmd_new(client, name="first", no_edit=True)
    create_migration_file(client.migrations_dir, "0001_first.sql", sql="SELECT 1;", rollback_sql="SELECT 1;")
    cmd_new(client, name="second", no_edit=True)
    create_migration_file(client.migrations_dir, "0002_second.sql", sql="SELECT 2;", rollback_sql="SELECT 2;")

    cmd_run(client=client)  # apply both

    result = cmd_squash(client=client, start_migration="0001", end_migration="0002", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in client.changelog.migrations]
    assert "0003_squashed_0001_0002.sql" in changelog_names
    assert "0001_first" not in changelog_names
    assert "0002_second" not in changelog_names

    # Verify squashed state in database
    if isinstance(client.connection, sqlite3.Connection):
        cursor = client.connection.execute("SELECT migration_name, squashed FROM migrations")
        applied = {row[0]: row[1] for row in cursor.fetchall()}
    else:
        with client.connection.cursor() as cursor:
            cursor.execute("SELECT migration_name, squashed FROM migrations")
            applied = {row[0]: row[1] for row in cursor.fetchall()}

    assert "0003_squashed_0001_0002.sql" in applied
    assert applied["0001_first.sql"] == 1
    assert applied["0002_second.sql"] == 1
