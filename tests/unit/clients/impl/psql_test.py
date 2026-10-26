from unittest.mock import MagicMock

import pytest

from migrateit.clients.psql import PsqlClient, _process_functions
from migrateit.models.changelog import Changelog
from migrateit.models.config import MigrateItConfig


def _make_mock_client() -> PsqlClient:
    mock_conn: MagicMock = MagicMock()
    changelog = Changelog(version=1, migrations=[])
    config = MigrateItConfig(changelog=changelog)
    return PsqlClient(mock_conn, config)


@pytest.mark.unit
def test_process_functions_invalid_definition() -> None:
    client = _make_mock_client()
    # Pass a row where func_def doesn't start with CREATE or ALTER
    rows: list[tuple[str, ...]] = [("public", "bad_func", "INVALID SQL STUFF", "")]
    with pytest.raises(ValueError, match="Invalid function definition"):
        _process_functions(client, rows)


@pytest.mark.unit
def test_process_functions_valid_function() -> None:
    client = _make_mock_client()
    rows: list[tuple[str, ...]] = [
        ("public", "my_func", "CREATE FUNCTION my_func() RETURNS INT AS $$ BEGIN RETURN 1; END; $$", "()")
    ]
    fwd, rb = _process_functions(client, rows)
    assert len(fwd) == 1
    assert fwd[0].startswith("CREATE FUNCTION")
    assert fwd[0].endswith(";")
    assert len(rb) == 1


@pytest.mark.unit
def test_process_functions_valid_procedure() -> None:
    client = _make_mock_client()
    rows: list[tuple[str, ...]] = [("public", "my_proc", "CREATE PROCEDURE my_proc()", "()")]
    fwd, rb = _process_functions(client, rows)
    assert len(fwd) == 1
    assert "PROCEDURE" in fwd[0]


@pytest.mark.unit
def test_process_functions_or_replace() -> None:
    client = _make_mock_client()
    rows: list[tuple[str, ...]] = [
        ("public", "my_func", "CREATE OR REPLACE FUNCTION my_func() RETURNS INT AS $$ $$", "()")
    ]
    fwd, rb = _process_functions(client, rows)
    assert len(fwd) == 1
    assert "OR REPLACE" in fwd[0]
