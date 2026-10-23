from pathlib import Path
from typing import Any

import pytest

from migrateit.clients._client import SqlClient
from migrateit.cmd import cmd_drop, cmd_new, cmd_run
from migrateit.models.migration import Migration
from tests.unit.cmd.conftest import _mock_client
from tests.unit.conftest import TEST_MIGRATIONS_TABLE, _create_migration_file, _get_query_rows


def test_cmd_drop(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql")

    cmd_drop(client, name="0001")
    assert not (Migration.migrations_dir / "0001_new.sql").exists()


def test_cmd_drop_applied(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", dependencies=("0000",), no_edit=True)
    _create_migration_file("0001_new.sql")

    cmd_run(client=client)
    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 2  # migrateit + new

    cmd_drop(client=client, name="0001")
    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 1


def test_cmd_drop_no_exists(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    (Migration.migrations_dir / "0001_new.sql").unlink()

    with pytest.raises(FileNotFoundError, match="does not exist"):
        cmd_drop(client, name="0001")


@pytest.mark.unit
def test_cmd_drop_is_parent(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    mock_client.changelog.migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_child.sql", parents=("0000_init.sql",)),
    ]
    assert cmd_drop(mock_client, name="0000_init.sql") != 0


@pytest.mark.unit
def test_cmd_drop_is_initial(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    mock_client.changelog.migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
    ]
    assert cmd_drop(mock_client, name="0000_init.sql") != 0


@pytest.mark.unit
def test_cmd_drop_file_not_exists(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    # Create a migration that's not a parent and not initial
    mock_client.changelog.migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_test.sql", parents=("0000_init.sql",)),
    ]
    # The file doesn't exist (we didn't create it) — get_full_path raises
    with pytest.raises(FileNotFoundError, match="does not exist"):
        cmd_drop(mock_client, name="0001_test.sql")


@pytest.mark.unit
def test_cmd_drop_file_no_sql(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    # Create a migration that's not a parent and not initial
    mock_client.changelog.migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_test.jpg", parents=("0000_init.sql",)),
    ]
    (temp_dir / "migrations" / "0001_test.jpg").touch()
    with pytest.raises(FileNotFoundError, match="not a valid SQL file"):
        cmd_drop(mock_client, name="0001_test.sql")
