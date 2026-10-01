import os
from pathlib import Path
from unittest.mock import patch

import pytest

from migrateit.cmd import cmd_new
from migrateit.tree import load_changelog_file

from .conftest import _mock_client


@pytest.mark.unit
def test_cmd_new_with_editor(temp_dir: Path) -> None:
    """Test cmd_new launches editor subprocess when no_edit is False."""
    mock_client = _mock_client(temp_dir)

    with (
        patch("migrateit.cmd.subprocess.call", return_value=0) as mock_call,
        patch.dict(os.environ, {"EDITOR": "vim"}, clear=False),
    ):
        result = cmd_new(mock_client, name="test_migration", no_edit=False)
        assert result == 0
        mock_call.assert_called_once()
        call_args = mock_call.call_args[0][0]
        assert "vim" in call_args
        assert "test_migration" in " ".join(call_args)


@pytest.mark.unit
def test_cmd_new_without_editor(temp_dir: Path) -> None:
    """Test cmd_new launches editor subprocess when no_edit is False."""
    mock_client = _mock_client(temp_dir)

    with (
        patch("migrateit.cmd.subprocess.call", return_value=0) as mock_call,
        patch.dict(os.environ, {"EDITOR": "vim"}, clear=False),
    ):
        result = cmd_new(mock_client, name="test_migration", no_edit=True)
        assert result == 0
        mock_call.assert_not_called()


@pytest.mark.unit
def test_cmd_new_with_existing_migration(temp_dir: Path) -> None:
    """Test cmd_new raises FileExistsError when migration file already exists."""
    mock_client = _mock_client(temp_dir)

    # Pre-create the migration file
    path = mock_client.migrations_dir / "0001_test_migration.sql"
    path.write_text("Hello, world!\n")

    with pytest.raises(FileExistsError, match="already exists"):
        cmd_new(client=mock_client, name="test_migration", no_edit=True)


@pytest.mark.unit
def test_cmd_new_with_dependencies(temp_dir: Path) -> None:
    """Test cmd_new creates migrations with explicit dependencies."""
    mock_client = _mock_client(temp_dir)

    cmd_new(client=mock_client, name="test_table", no_edit=True)
    cmd_new(client=mock_client, name="test_table2", dependencies=["0000", "0001"], no_edit=True)

    assert (mock_client.migrations_dir / "0001_test_table.sql").exists()

    changelog = load_changelog_file(mock_client.changelog.path)
    assert len(changelog.migrations) == 3
    assert changelog.migrations[2].name == "0002_test_table2.sql"
    assert changelog.migrations[2].parents == ["0000_migrateit.sql", "0001_test_table.sql"]
