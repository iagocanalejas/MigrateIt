from typing import Any

import pytest

from migrateit.clients._client import SqlClient
from migrateit.cmd import cmd_new, cmd_run, cmd_squash
from tests.conftest import _create_migration_file, _get_query_rows


def test_cmd_squash_all_applied(client: SqlClient[Any]) -> None:
    cmd_new(client, name="first", no_edit=True)
    _create_migration_file(client.migrations_dir, "0001_first.sql")
    cmd_new(client, name="second", no_edit=True)
    _create_migration_file(client.migrations_dir, "0002_second.sql")

    cmd_run(client)  # apply both

    result = cmd_squash(client, start_migration="0001", end_migration="0002", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in client.changelog.migrations]
    assert "0003_squashed_0001_0002.sql" in changelog_names
    assert "0001_first" not in changelog_names
    assert "0002_second" not in changelog_names

    # Verify squashed state in database
    rows = _get_query_rows(client, "SELECT migration_name, squashed FROM migrations")
    applied = {row[0]: row[1] for row in rows}

    assert "0003_squashed_0001_0002.sql" in applied
    assert applied["0001_first.sql"] == 1
    assert applied["0002_second.sql"] == 1


def test_cmd_squash_not_all_applied(client: SqlClient[Any]) -> None:
    cmd_new(client, name="first", no_edit=True)
    _create_migration_file(client.migrations_dir, "0001_first.sql")

    cmd_new(client, name="second", no_edit=True)
    _create_migration_file(client.migrations_dir, "0002_second.sql")

    result = cmd_squash(client, start_migration="0001", end_migration="0002", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in client.changelog.migrations]
    assert "0003_squashed_0001_0002.sql" in changelog_names
    assert "0001_first" not in changelog_names
    assert "0002_second" not in changelog_names


def test_cmd_squash_no_end_too_few_migrations(client: SqlClient[Any]) -> None:
    """Test cmd_squash raises ValueError when end_migration is None and fewer than 3 migrations."""
    with pytest.raises(ValueError, match="squash less than 3"):
        cmd_squash(client, start_migration="0000_migrateit.sql")


def test_cmd_squash_no_end_uses_last_migration(client: SqlClient[Any]) -> None:
    """Test cmd_squash uses the last migration as end_migration when not provided."""
    cmd_new(client, name="first", no_edit=True)
    _create_migration_file(client.migrations_dir, "0001_first.sql")

    cmd_new(client, name="second", no_edit=True)
    _create_migration_file(client.migrations_dir, "0002_second.sql")

    result = cmd_squash(client, start_migration="0001", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in client.changelog.migrations]
    assert "0003_squashed_0001_0002.sql" in changelog_names
    assert "0001_first" not in changelog_names
    assert "0002_second" not in changelog_names
