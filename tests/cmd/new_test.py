import os
from pathlib import Path
from unittest.mock import patch

import pytest

from migrateit.cmd import cmd_new
from migrateit.models.changelog import load_changelog_file

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


@pytest.mark.unit
def test_cmd_new_interactive_with_dependencies_raises_value_error(temp_dir: Path) -> None:
    """Test cmd_new raises ValueError when both interactive mode and explicit dependencies are passed."""
    mock_client = _mock_client(temp_dir)

    with pytest.raises(ValueError, match="Cannot specify both `--interactive` and `--dependencies`"):
        cmd_new(
            client=mock_client,
            name="test_migration",
            dependencies=["0000_migrateit.sql"],
            interactive=True,
            no_edit=True,
        )


@pytest.mark.unit
def test_cmd_new_interactive_selection(temp_dir: Path) -> None:
    """Test cmd_new prompts user for dependencies when interactive is True."""
    mock_client = _mock_client(temp_dir)

    prompt_result = {"dependencies": ["0000_migrateit.sql"]}

    with patch("migrateit.cmd.inquirer.prompt", return_value=prompt_result) as mock_prompt:
        cmd_new(client=mock_client, name="interactive_migration", interactive=True, no_edit=True)

        mock_prompt.assert_called_once()
        # Verify choices were generated from existing migrations
        checkbox = mock_prompt.call_args[0][0][0]
        assert checkbox.choices == ["0000_migrateit.sql"]

    changelog = load_changelog_file(mock_client.changelog.path)
    new_migration = changelog.migrations[-1]
    assert new_migration.name == "0001_interactive_migration.sql"
    assert "0000_migrateit.sql" in new_migration.parents


@pytest.mark.unit
def test_cmd_new_interactive_cancelled(temp_dir: Path) -> None:
    """Test cmd_new gracefully handles prompt cancellation (None returned from inquirer)."""
    mock_client = _mock_client(temp_dir)

    with patch("migrateit.cmd.inquirer.prompt", return_value=None) as mock_prompt:
        result = cmd_new(client=mock_client, name="cancelled_migration", interactive=True, no_edit=True)

        assert result == 0
        mock_prompt.assert_called_once()

    changelog = load_changelog_file(mock_client.changelog.path)
    assert changelog.migrations[-1].name == "0001_cancelled_migration.sql"
