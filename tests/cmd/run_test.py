from typing import Any

import pytest

from migrateit.clients._client import SqlClient
from migrateit.cmd import cmd_new, cmd_run
from tests.conftest import _create_migration_file, _get_query_rows, _table_exists


def test_cmd_run_and_rerun(client: SqlClient[Any]) -> None:
    """Test cmd_run applies a new migration and is a no-op on re-run."""
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file(client.migrations_dir, "0001_new.sql")

    cmd_run(client=client)
    rows = _get_query_rows(client, "SELECT migration_name FROM migrations")
    assert len(rows) == 2  # migrateit + new

    cmd_run(client=client)
    rows = _get_query_rows(client, "SELECT migration_name FROM migrations")
    assert len(rows) == 2


def test_cmd_run_by_name_not_found(client: SqlClient[Any]) -> None:
    """Test cmd_run raises ValueError for non-existent target migration."""
    with pytest.raises(ValueError) as ctx:
        cmd_run(client=client, name="0010")
    assert "Migration '0010' not found" in str(ctx.value)


def test_cmd_run_fake(client: SqlClient[Any]) -> None:
    """Test fake migration marks as applied but does not execute SQL."""
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file(client.migrations_dir, "0001_new.sql", sql="CREATE TABLE test (id INTEGER PRIMARY KEY);")

    cmd_new(client, name="new", no_edit=True)
    _create_migration_file(client.migrations_dir, "0002_new.sql", sql="CREATE TABLE test2 (id INTEGER PRIMARY KEY);")

    cmd_run(client=client, is_fake=True)
    rows = _get_query_rows(client, "SELECT migration_name FROM migrations")
    assert len(rows) == 3

    # Verify table was NOT created (fake doesn't execute SQL)
    # Use DB-specific table existence check
    assert not _table_exists(client, "test")
    assert not _table_exists(client, "test2")


def test_cmd_run_rollback(client: SqlClient[Any]) -> None:
    """Test cmd_run applies then rolls back a migration."""
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file(
        client.migrations_dir,
        "0001_new.sql",
        sql="CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY);",
        rollback_sql="DROP TABLE test;",
    )

    cmd_run(client=client, name="0001")
    rows = _get_query_rows(client, "SELECT migration_name FROM migrations")
    assert len(rows) == 2

    cmd_run(client=client, name="0001", is_rollback=True)
    rows = _get_query_rows(client, "SELECT migration_name FROM migrations")
    assert len(rows) == 1


def test_cmd_run_no_migrations_to_rollback(client: SqlClient[Any]) -> None:
    """Test cmd_run returns early when nothing to roll back."""
    cmd_run(client=client, name="0000_migrateit.sql", is_rollback=True)


def test_cmd_run_all_applied(client: SqlClient[Any]) -> None:
    """Test cmd_run returns early when all migrations already applied."""
    cmd_run(client=client)


def test_cmd_run_hash_update_no_target(client: SqlClient[Any]) -> None:
    """Test cmd_run raises ValueError when hash_update is True but no name given."""
    with pytest.raises(ValueError) as ctx:
        cmd_run(client=client, is_hash_update=True)
    assert "Hash update requires a target migration name" in str(ctx.value)


def test_cmd_run_hash_update_initial(client: SqlClient[Any]) -> None:
    """Test cmd_run raises ValueError when trying to update hash of the initial migration."""
    initial = [m for m in client.changelog.migrations if m.initial][0]
    with pytest.raises(ValueError) as ctx:
        cmd_run(client=client, name=initial.name, is_hash_update=True)
    assert "Cannot update hash for the initial migration" in str(ctx.value)


def test_cmd_run_hash_update_success(client: SqlClient[Any]) -> None:
    """Test cmd_run successfully updates the hash for a non-initial migration."""
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file(client.migrations_dir, "0001_new.sql")

    cmd_run(client=client, name="0001")
    rows_before = _get_query_rows(client, "SELECT migration_name, change_hash FROM migrations")
    original_hash = next(r[1] for r in rows_before if r[0] == "0001_new.sql")

    cmd_run(client=client, name="0001", is_hash_update=True)
    rows_after = _get_query_rows(client, "SELECT migration_name, change_hash FROM migrations")
    updated_hash = next(r[1] for r in rows_after if r[0] == "0001_new.sql")

    assert updated_hash == original_hash


def test_cmd_run_rollback_on_error(client: SqlClient[Any]) -> None:
    """Test cmd_run calls connection.rollback() when apply_migration raises an exception."""
    from unittest.mock import MagicMock, patch

    cmd_new(client, name="first", no_edit=True)
    cmd_new(client, name="second", no_edit=True)
    _create_migration_file(client.migrations_dir, "0001_first.sql")
    _create_migration_file(client.migrations_dir, "0002_second.sql")

    side_effect_calls: list[int] = [0]

    def _apply_then_fail(*_, **__) -> None:  # type: ignore
        side_effect_calls[0] += 1
        if side_effect_calls[0] == 2:
            raise RuntimeError("simulated DB error")

    mock_conn = MagicMock()

    with (
        patch.object(client, "apply_migration", side_effect=_apply_then_fail),
        patch.object(client, "connection", mock_conn),
    ):
        with pytest.raises(RuntimeError, match="simulated DB error"):
            cmd_run(client=client)
        mock_conn.rollback.assert_called_once()
