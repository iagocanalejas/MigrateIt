from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from migrateit.clients.mysql import MySqlClient, _process_pk_unique, _process_routines
from migrateit.models.changelog import Changelog
from migrateit.models.config import MigrateItConfig


def _make_mock_client() -> MySqlClient:
    mock_conn: MagicMock = MagicMock()
    changelog = Changelog(version=1, migrations=[])
    config = MigrateItConfig(changelog=changelog)
    return MySqlClient(mock_conn, config)


@pytest.mark.unit
def test_process_routines_unexpected_type() -> None:
    client = _make_mock_client()
    # Pass a row with an unexpected routine type
    rows: list[tuple[str, ...]] = [("mydb", "myroutine", "TRIGGER")]
    with pytest.raises(ValueError, match="Unexpected routine type"):
        _process_routines(client, rows)


@pytest.mark.unit
def test_process_routines_valid_function() -> None:
    client = _make_mock_client()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.fetchone.return_value = (
        "schema",
        "name",
        "CREATE FUNCTION myfunc() RETURNS INT RETURN 1;",
    )

    rows: list[tuple[str, ...]] = [("mydb", "myfunc", "FUNCTION")]
    fwd, rb = _process_routines(client, rows)
    assert len(fwd) == 1
    assert len(rb) == 1


@pytest.mark.unit
def test_process_routines_valid_procedure() -> None:
    client = _make_mock_client()
    mock_conn = MagicMock()
    client.connection = mock_conn
    mock_conn.cursor.return_value.fetchone.return_value = (
        "schema",
        "name",
        "CREATE PROCEDURE myproc() BEGIN END;",
    )

    rows: list[tuple[str, ...]] = [("mydb", "myproc", "PROCEDURE")]
    fwd, rb = _process_routines(client, rows)
    assert len(fwd) == 1
    assert len(rb) == 1


@pytest.mark.unit
def test_process_pk_unique_unknown_constraint_type() -> None:
    client = _make_mock_client()
    # Pass a row with an unsupported constraint type (not "PRIMARY KEY" or "UNIQUE")
    rows: list[tuple[str, ...]] = [("mydb", "mytable", "mycon", "CHECK", "id > 0")]
    with pytest.raises(ValueError, match="Unsupported constraint type"):
        _process_pk_unique(client, rows)
