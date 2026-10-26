from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from migrateit.clients._client import SqlClient
from migrateit.clients._lock import _DummyLocker
from migrateit.cmd import cmd_new, cmd_run
from tests.unit.cmd.conftest import _mock_client
from tests.unit.conftest import TEST_MIGRATIONS_TABLE, _create_migration_file, _get_query_rows, _table_exists

# --- migrate ---


def test_cmd_run_and_rerun(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql")

    cmd_run(client=client)
    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 2

    cmd_run(client=client)
    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 2


def test_cmd_run_all_applied(client: SqlClient[Any]) -> None:
    cmd_run(client=client)
    with patch("migrateit.cmd.run.write_line") as mock_write_line:
        cmd_run(client=client)
    mock_write_line.assert_called_once_with("All migrations already applied.")


def test_cmd_run_fake(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql", sql="CREATE TABLE test (id INTEGER PRIMARY KEY);")

    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0002_new.sql", sql="CREATE TABLE test2 (id INTEGER PRIMARY KEY);")

    cmd_run(client=client, is_fake=True)
    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 3
    assert not _table_exists(client, "test")
    assert not _table_exists(client, "test2")


def test_cmd_run_plan_only(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql", sql="CREATE TABLE test (id INTEGER PRIMARY KEY);")

    cmd_run(client=client, is_plan_only=True)
    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 1
    assert not _table_exists(client, "test")


def test_cmd_run_interactive(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql", sql="CREATE TABLE test (id INTEGER PRIMARY KEY);")

    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0002_new.sql", sql="CREATE TABLE test2 (id INTEGER PRIMARY KEY);")

    prompt_result = {"migrations": ["0001_new.sql"]}
    with patch("migrateit.cmd.run.inquirer.prompt", return_value=prompt_result):
        cmd_run(client=client, interactive=True)

    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 2
    assert _table_exists(client, "test")
    assert not _table_exists(client, "test2")


def test_cmd_run_interactive_parents_not_applied(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql", sql="CREATE TABLE test (id INTEGER PRIMARY KEY);")

    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0002_new.sql", sql="CREATE TABLE test2 (id INTEGER PRIMARY KEY);")

    prompt_result = {"migrations": ["0002_new.sql"]}
    with patch("migrateit.cmd.run.inquirer.prompt", return_value=prompt_result):
        cmd_run(client=client, interactive=True)

    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 1
    assert not _table_exists(client, "test")
    assert not _table_exists(client, "test2")


def test_cmd_run_interactive_noop(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql", sql="CREATE TABLE test (id INTEGER PRIMARY KEY);")

    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0002_new.sql", sql="CREATE TABLE test2 (id INTEGER PRIMARY KEY);")

    prompt_result: dict[str, list[str]] = {"migrations": []}
    with patch("migrateit.cmd.run.inquirer.prompt", return_value=prompt_result):
        cmd_run(client=client, interactive=True)

    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 1
    assert not _table_exists(client, "test")
    assert not _table_exists(client, "test2")


# --- rollback ---


def test_cmd_run_rollback(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file(
        "0001_new.sql",
        sql="CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY);",
        rollback_sql="DROP TABLE test;",
    )

    cmd_run(client=client, name="0001")
    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 2

    cmd_run(client=client, name="0001", is_rollback=True)
    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 1


def test_cmd_run_rollback_interactive(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql", sql="CREATE TABLE test (id INTEGER PRIMARY KEY);")

    cmd_new(client, name="new", no_edit=True)
    _create_migration_file(
        "0002_new.sql",
        sql="CREATE TABLE test2 (id INTEGER PRIMARY KEY);",
        rollback_sql="DROP TABLE test2;",
    )

    cmd_run(client=client)
    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 3

    prompt_result = {"migrations": ["0002_new.sql"]}
    with patch("migrateit.cmd.run.inquirer.prompt", return_value=prompt_result):
        cmd_run(client=client, is_rollback=True, interactive=True)

    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 2
    assert _table_exists(client, "test")
    assert not _table_exists(client, "test2")


def test_cmd_run_rollback_interactive_childs_not_rolled_back(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql", sql="CREATE TABLE test (id INTEGER PRIMARY KEY);")

    cmd_new(client, name="new", no_edit=True)
    _create_migration_file(
        "0002_new.sql",
        sql="CREATE TABLE test2 (id INTEGER PRIMARY KEY);",
        rollback_sql="DROP TABLE test2;",
    )

    cmd_run(client=client)
    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 3

    prompt_result = {"migrations": ["0001_new.sql"]}
    with patch("migrateit.cmd.run.inquirer.prompt", return_value=prompt_result):
        cmd_run(client=client, is_rollback=True, interactive=True)

    rows = _get_query_rows(client, f"SELECT migration_name FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    assert len(rows) == 3
    assert _table_exists(client, "test")
    assert _table_exists(client, "test2")


def test_cmd_run_full_rollback(client: SqlClient[Any]) -> None:
    cmd_run(client, name="0000", is_rollback=True)
    assert not _table_exists(client, TEST_MIGRATIONS_TABLE)


def test_cmd_run_rollbacks_on_error(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql", sql="INVALID SQL")

    spy_connection = MagicMock(wraps=client.connection)
    client.connection = spy_connection

    with patch("migrateit.cmd.run.DatabaseLock._build_locker", return_value=_DummyLocker(client, client.table_name)):
        with pytest.raises(Exception):
            cmd_run(client, name="0001")
        spy_connection.rollback.assert_called_once()


@pytest.mark.unit
def test_cmd_run_rollback_no_name(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    assert cmd_run(mock_client, is_rollback=True) != 0


def test_cmd_run_by_name_not_found(client: SqlClient[Any]) -> None:
    with pytest.raises(ValueError, match="not found"):
        cmd_run(client=client, name="0010")


def test_cmd_run_no_migrations_to_rollback(client: SqlClient[Any]) -> None:
    cmd_run(client=client)

    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql")

    with patch("migrateit.cmd.run.write_line") as mock_write_line:
        cmd_run(client=client, name="0001", is_rollback=True)
    mock_write_line.assert_called_with("No migrations to roll back.")


# --- hash update ---


def test_cmd_run_hash_update_success(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql")

    cmd_run(client=client, name="0001")
    rows_before = _get_query_rows(client, f"SELECT migration_name, change_hash FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    original_hash = next(r[1] for r in rows_before if r[0] == "0001_new.sql")

    cmd_run(client=client, name="0001", is_hash_update=True)
    rows_after = _get_query_rows(client, f"SELECT migration_name, change_hash FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    updated_hash = next(r[1] for r in rows_after if r[0] == "0001_new.sql")

    assert updated_hash == original_hash


def test_cmd_run_hash_interactive(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql")

    cmd_run(client=client, name="0001")
    rows_before = _get_query_rows(client, f"SELECT migration_name, change_hash FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    original_hash = next(r[1] for r in rows_before if r[0] == "0001_new.sql")

    prompt_result = {"target": "0001_new.sql"}
    with patch("migrateit.cmd.run.inquirer.prompt", return_value=prompt_result):
        cmd_run(client=client, is_hash_update=True, interactive=True)

    rows_after = _get_query_rows(client, f"SELECT migration_name, change_hash FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    updated_hash = next(r[1] for r in rows_after if r[0] == "0001_new.sql")

    assert updated_hash == original_hash


def test_cmd_run_hash_interactive_with_name(client: SqlClient[Any]) -> None:
    cmd_new(client, name="new", no_edit=True)
    _create_migration_file("0001_new.sql")

    cmd_run(client=client, name="0001")
    rows_before = _get_query_rows(client, f"SELECT migration_name, change_hash FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    original_hash = next(r[1] for r in rows_before if r[0] == "0001_new.sql")

    prompt_result = {"target": "0001_new.sql"}
    with patch("migrateit.cmd.run.inquirer.prompt", return_value=prompt_result):
        cmd_run(client=client, name="0001", is_hash_update=True, interactive=True)

    rows_after = _get_query_rows(client, f"SELECT migration_name, change_hash FROM {client._q(TEST_MIGRATIONS_TABLE)}")
    updated_hash = next(r[1] for r in rows_after if r[0] == "0001_new.sql")

    assert updated_hash == original_hash


def test_cmd_run_hash_update_no_target(client: SqlClient[Any]) -> None:
    assert cmd_run(client=client, is_hash_update=True) != 0


def test_cmd_run_hash_update_initial(client: SqlClient[Any]) -> None:
    initial = [m for m in client.changelog.migrations if m.initial][0]
    assert cmd_run(client=client, name=initial.name, is_hash_update=True) != 0


def test_cmd_run_hash_update_fake(client: SqlClient[Any]) -> None:
    assert cmd_run(client=client, is_fake=True, is_hash_update=True) != 0


def test_cmd_run_hash_rollback(client: SqlClient[Any]) -> None:
    assert cmd_run(client=client, is_rollback=True, is_hash_update=True) != 0
