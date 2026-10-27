from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from migrateit.clients._client import SqlClient
from migrateit.cmd import cmd_new, cmd_run, cmd_squash
from migrateit.models.migration import Migration, MigrationStatus
from tests.unit.clients.conftest import _get_query_rows
from tests.unit.cmd.conftest import _mock_client
from tests.unit.conftest import INITIAL_MIGRATION, TEST_MIGRATIONS_TABLE, _create_migration_file


def test_cmd_squash_all_applied(client: SqlClient[Any]) -> None:
    cmd_new(client, name="first", no_edit=True)
    _create_migration_file("0001_first.sql")
    cmd_new(client, name="second", no_edit=True)
    _create_migration_file("0002_second.sql")
    cmd_new(client, name="third", no_edit=True)
    _create_migration_file("0003_third.sql")

    cmd_run(client)  # apply all

    result = cmd_squash(client, start_migration_name="0001", end_migration_name="0002", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in client.changelog.migrations]
    assert changelog_names == [INITIAL_MIGRATION, "0004_squashed_0001_0002.sql", "0003_third.sql"]

    # Verify parents got updated
    assert client.changelog.migrations[2].parents == ("0004_squashed_0001_0002.sql",)

    # Verify migrations got renamed
    assert Migration.migrations_dir / "0001_first.sql" not in Migration.migrations_dir.iterdir()
    assert Migration.migrations_dir / "squashed_0001_first.sql" in Migration.migrations_dir.iterdir()

    # Verify squashed state in database
    rows = _get_query_rows(client, f"SELECT migration_name, squashed FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    applied = {row[0]: row[1] for row in rows}

    assert "0004_squashed_0001_0002.sql" in applied
    assert applied["0001_first.sql"] == 1
    assert applied["0002_second.sql"] == 1


def test_cmd_squash_not_all_applied(client: SqlClient[Any]) -> None:
    cmd_new(client, name="first", no_edit=True)
    _create_migration_file("0001_first.sql")

    cmd_new(client, name="second", no_edit=True)
    _create_migration_file("0002_second.sql")

    result = cmd_squash(client, start_migration_name="0001", end_migration_name="0002", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in client.changelog.migrations]
    assert "0003_squashed_0001_0002.sql" in changelog_names
    assert "0001_first" not in changelog_names
    assert "0002_second" not in changelog_names


def test_cmd_squash_fails_with_empty_migration(client: SqlClient[Any]) -> None:
    cmd_new(client, name="first", no_edit=True)
    _create_migration_file("0001_first.sql")

    cmd_new(client, name="second", no_edit=True)
    _create_migration_file("0002_second.sql", sql="", rollback_sql="")

    result = cmd_squash(client, start_migration_name="0001", end_migration_name="0002", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in client.changelog.migrations]
    assert "0003_squashed_0001_0002.sql" in changelog_names
    assert "0001_first" not in changelog_names
    assert "0002_second" not in changelog_names


def test_cmd_squash_no_end_too_few_migrations(client: SqlClient[Any]) -> None:
    assert cmd_squash(client, start_migration_name="0000_migrateit.sql") != 0


def test_cmd_squash_no_end_uses_last_migration(client: SqlClient[Any]) -> None:
    cmd_new(client, name="first", no_edit=True)
    _create_migration_file("0001_first.sql")

    cmd_new(client, name="second", no_edit=True)
    _create_migration_file("0002_second.sql")

    result = cmd_squash(client, start_migration_name="0001", name="squashed_0001_0002")
    assert result == 0

    changelog_names = [m.name for m in client.changelog.migrations]
    assert "0003_squashed_0001_0002.sql" in changelog_names
    assert "0001_first" not in changelog_names
    assert "0002_second" not in changelog_names


@pytest.mark.unit
def test_cmd_squash_no_migrations_table(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    mock_client.is_migrations_table_created.return_value = False  # type: ignore
    assert cmd_squash(mock_client, start_migration_name="0001") != 0


@pytest.mark.unit
def test_cmd_squash_no_path_found(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    mock_client.is_migrations_table_created.return_value = True  # type: ignore
    # Add migrations to changelog so get_migration_by_name doesn't fail first
    mock_client.changelog.migrations = [
        Migration(name="0000_migrateit.sql", initial=True, parents=()),
        Migration(name="0001_a.sql", parents=("0000_migrateit.sql",)),
    ]
    # Patch find_path on the real MigrationManager to return no path
    with patch.object(mock_client.migrations, "find_path", return_value=[]):
        assert cmd_squash(mock_client, start_migration_name="0001_a", end_migration_name="0001_a") != 0


@pytest.mark.unit
def test_cmd_squash_initial_migration(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    mock_client.is_migrations_table_created.return_value = True  # type: ignore
    # Add more migrations so we have 3+ (required for squash without end_migration)
    mock_client.changelog.migrations.append(
        Migration(name="0001_second.sql", parents=("0000_migrateit.sql",)),
    )
    mock_client.changelog.migrations.append(
        Migration(name="0002_third.sql", parents=("0001_second.sql",)),
    )
    assert cmd_squash(mock_client, start_migration_name="0000_migrateit.sql") != 0


@pytest.mark.unit
def test_cmd_squash_different_states(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    mock_client.is_migrations_table_created.return_value = True  # type: ignore
    mock_client.changelog.migrations = [
        Migration(name="0000_migrateit.sql", initial=True, parents=()),
        Migration(name="0001_a.sql", parents=("0000_migrateit.sql",)),
        Migration(name="0002_b.sql", parents=("0001_a.sql",)),
    ]
    # Make one applied and one not applied
    mock_client.retrieve_migration_statuses.return_value = {  # type: ignore
        "0000_migrateit.sql": MigrationStatus.APPLIED,
        "0001_a.sql": MigrationStatus.APPLIED,
        "0002_b.sql": MigrationStatus.NOT_APPLIED,
    }
    assert cmd_squash(mock_client, start_migration_name="0001_a", end_migration_name="0002_b") != 0
