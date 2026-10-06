from typing import Any

import pytest

from migrateit.clients._client import SqlClient
from migrateit.cmd import cmd_drop, cmd_new, cmd_run
from tests.conftest import _create_migration_file, _get_query_rows


def test_cmd_drop(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file(client.migrations_dir, "0001_new.sql")

    cmd_drop(client, name="0001")
    assert not (client.migrations_dir / "0001_new.sql").exists()


def test_cmd_drop_applied(client: SqlClient[Any]) -> None:
    """Test cmd_run applies a new migration and is a no-op on re-run."""
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file(client.migrations_dir, "0001_new.sql")

    cmd_run(client=client)
    rows = _get_query_rows(client, "SELECT migration_name FROM migrations")
    assert len(rows) == 2  # migrateit + new

    cmd_drop(client=client, name="0001")
    rows = _get_query_rows(client, "SELECT migration_name FROM migrations")
    assert len(rows) == 1


def test_cmd_drop_no_exists(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    (client.migrations_dir / "0001_new.sql").unlink()

    with pytest.raises(FileNotFoundError, match="does not exist"):
        cmd_drop(client, name="0001")
