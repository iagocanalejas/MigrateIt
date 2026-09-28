import sqlite3
from pathlib import Path
from typing import Any

import pytest

from migrateit.cli import cmd_new, cmd_run
from migrateit.clients import SqlClient
from tests.cli.conftest import get_query_rows
from tests.conftest import create_migration_file


@pytest.mark.integration
def test_cmd_run_and_rerun(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test cmd_run applies a new migration and is a no-op on re-run."""
    client = cmd_client
    cmd_new(client, name="new", no_edit=True)
    create_migration_file(client.migrations_dir, "0001_new.sql")

    cmd_run(client=client)
    rows = get_query_rows(client.connection, "SELECT migration_name FROM migrations")
    assert len(rows) == 2  # migrateit + new

    cmd_run(client=client)
    rows = get_query_rows(client.connection, "SELECT migration_name FROM migrations")
    assert len(rows) == 2


@pytest.mark.integration
def test_cmd_run_by_name_not_found(cmd_client: SqlClient[Any]) -> None:
    """Test cmd_run raises ValueError for non-existent target migration."""
    with pytest.raises(ValueError) as ctx:
        cmd_run(client=cmd_client, name="0010")
    assert "Migration '0010' not found" in str(ctx.value)


@pytest.mark.integration
def test_cmd_run_fake(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test fake migration marks as applied but does not execute SQL."""
    client = cmd_client
    cmd_new(client, name="new", no_edit=True)
    create_migration_file(client.migrations_dir, "0001_new.sql", sql="CREATE TABLE test (id INTEGER PRIMARY KEY);")

    cmd_run(client=client, name="0001", is_fake=True)
    rows = get_query_rows(client.connection, "SELECT migration_name FROM migrations")
    assert len(rows) == 1  # only new migration (0001 marked fake)

    # Verify table was NOT created (fake doesn't execute SQL)
    # Use DB-specific table existence check
    if isinstance(client.connection, sqlite3.Connection):
        sqlite_cursor = client.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='test';",
        )
        assert sqlite_cursor.fetchone() is None
    else:
        import psycopg

        assert isinstance(client.connection, psycopg.Connection)
        with client.connection.cursor() as pg_cursor:
            pg_cursor.execute(
                "SELECT EXISTS("
                "  SELECT 1 FROM information_schema.tables "
                "  WHERE table_name = 'test' AND table_schema = 'public'"
                ");",
            )
            result = pg_cursor.fetchone()
            assert result is not None and not result[0]


@pytest.mark.integration
def test_cmd_run_rollback(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test cmd_run applies then rolls back a migration."""
    client = cmd_client
    cmd_new(client, name="new", no_edit=True)
    create_migration_file(
        client.migrations_dir,
        "0001_new.sql",
        sql="CREATE TABLE test (id INTEGER PRIMARY KEY);",
        rollback_sql="DROP TABLE test;",
    )

    cmd_run(client=client, name="0001")
    rows = get_query_rows(client.connection, "SELECT migration_name FROM migrations")
    assert len(rows) == 2

    cmd_run(client=client, name="0001", is_rollback=True)
    rows = get_query_rows(client.connection, "SELECT migration_name FROM migrations")
    assert len(rows) == 1


@pytest.mark.integration
def test_cmd_run_no_migrations_to_rollback(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test cmd_run returns early when nothing to roll back."""
    client = cmd_client
    cmd_run(client=client, name="0000_migrateit.sql", is_rollback=True)


@pytest.mark.integration
def test_cmd_run_all_applied(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test cmd_run returns early when all migrations already applied."""
    client = cmd_client
    cmd_run(client=client)


def test_cmd_run_hash_update_no_target(cmd_client: SqlClient[Any]) -> None:
    """Test cmd_run raises ValueError when hash_update is True but no name given."""
    with pytest.raises(ValueError) as ctx:
        cmd_run(client=cmd_client, is_hash_update=True)
    assert "Hash update requires a target migration name" in str(ctx.value)


def test_cmd_run_hash_update_initial(cmd_client: SqlClient[Any]) -> None:
    """Test cmd_run raises ValueError when trying to update hash of the initial migration."""
    initial = [m for m in cmd_client.changelog.migrations if m.initial][0]
    with pytest.raises(ValueError) as ctx:
        cmd_run(client=cmd_client, name=initial.name, is_hash_update=True)
    assert "Cannot update hash for the initial migration" in str(ctx.value)


@pytest.mark.integration
def test_cmd_run_hash_update_success(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test cmd_run successfully updates the hash for a non-initial migration."""
    client = cmd_client
    cmd_new(client, name="new", no_edit=True)
    create_migration_file(client.migrations_dir, "0001_new.sql")

    cmd_run(client=client, name="0001")
    rows_before = get_query_rows(client.connection, "SELECT migration_name, change_hash FROM migrations")
    original_hash = next(r[1] for r in rows_before if r[0] == "0001_new.sql")

    cmd_run(client=client, name="0001", is_hash_update=True)
    rows_after = get_query_rows(client.connection, "SELECT migration_name, change_hash FROM migrations")
    updated_hash = next(r[1] for r in rows_after if r[0] == "0001_new.sql")

    assert updated_hash == original_hash
