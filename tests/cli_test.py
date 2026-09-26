import os
import sqlite3
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from migrateit.clients.psql import PsqlClient
from migrateit.clients.sqlite import SqliteClient
from migrateit.main import _get_connection, main
from migrateit.models.changelog import SupportedDatabase


@pytest.mark.postgres
def test_get_connection_postgres() -> None:
    """Test _get_connection returns a psycopg connection for PostgreSQL."""
    with patch("migrateit.main.psycopg.connect") as mock_connect:
        mock_conn: Any = MagicMock()
        mock_connect.return_value = mock_conn
        result = _get_connection(SupportedDatabase.POSTGRES)
        assert result == mock_conn
        mock_connect.assert_called_once()
        mock_conn.autocommit = False


@pytest.mark.sqlite
def test_get_connection_sqlite() -> None:
    """Test _get_connection returns a sqlite3 connection for SQLite."""
    with patch.object(SqliteClient, "get_environment_url", return_value="sqlite:///./test.db"):
        result = _get_connection(SupportedDatabase.SQLITE)
        assert isinstance(result, sqlite3.Connection)
        result.close()


@pytest.mark.unit
def test_main_no_args_returns_1(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test main() returns 1 when no subcommand is provided."""
    monkeypatch.setattr(sys, "argv", ["migrateit"])
    with (
        patch("migrateit.main.print_logo"),
        patch("migrateit.main.error_handler"),
        patch("migrateit.main.logging_handler"),
        patch("sys.stdout"),
    ):
        result = main()
        assert result == 1


@pytest.mark.unit
def test_main_version_flag(monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]) -> None:
    """Test main() exits with version output when -V flag is passed."""
    monkeypatch.setattr(sys, "argv", ["migrateit", "-V"])
    with pytest.raises(SystemExit):
        main()
    captured = capfd.readouterr()
    assert "migrateit" in captured.out


@pytest.mark.unit
def test_cmd_new_with_editor(temp_dir: Path) -> None:
    """Test cmd_new launches editor subprocess when no_edit is False."""
    from migrateit.cli import cmd_new

    mock_client: MagicMock = MagicMock(spec=PsqlClient)
    mock_client.is_migrations_table_created.return_value = True
    mock_client.changelog.migrations = [MagicMock(name="0000_init.sql", initial=True, parents=[])]
    mock_client.migrations_dir = temp_dir / "migrations"
    mock_client.migrations_dir.mkdir(parents=True, exist_ok=True)

    with (
        patch("migrateit.cli.subprocess.call", return_value=0) as mock_call,
        patch.dict(os.environ, {"EDITOR": "vim"}, clear=False),
    ):
        result = cmd_new(mock_client, name="test_migration", no_edit=False)
        assert result == 0
        mock_call.assert_called_once()
        call_args = mock_call.call_args[0][0]
        assert "vim" in call_args
        assert "test_migration" in " ".join(call_args)
