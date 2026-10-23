import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from migrateit.cmd import _validate_editor, cmd_new
from migrateit.models.migration import Migration
from migrateit.models.repository import ChangelogRepository

from .conftest import _mock_client


@pytest.mark.unit
def test_cmd_new_with_editor(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)

    with (
        patch("migrateit.cmd.subprocess.run", return_value=MagicMock(returncode=0)) as mock_call,
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
    mock_client = _mock_client(temp_dir)

    with (
        patch("migrateit.cmd.subprocess.run", return_value=MagicMock(returncode=0)) as mock_call,
        patch.dict(os.environ, {"EDITOR": "vim"}, clear=False),
    ):
        result = cmd_new(mock_client, name="test_migration", no_edit=True)
        assert result == 0
        mock_call.assert_not_called()


@pytest.mark.unit
def test_cmd_new_with_existing_migration(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)

    # Pre-create the migration file
    path = Migration.migrations_dir / "0001_test_migration.sql"
    path.write_text("Hello, world!\n")

    with patch("migrateit.models.migration_manager.inquirer.confirm", return_value=False):
        with pytest.raises(FileExistsError, match="already exists"):
            cmd_new(client=mock_client, name="test_migration", no_edit=True)


@pytest.mark.unit
def test_cmd_new_overwrite_migration(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)

    # Pre-create the migration file
    path = Migration.migrations_dir / "0001_test_migration.sql"
    path.write_text("Hello, world!\n")

    with patch("migrateit.models.migration_manager.inquirer.confirm", return_value=True):
        cmd_new(client=mock_client, name="test_migration", no_edit=True)

    assert (Migration.migrations_dir / "0001_test_migration.sql").exists()
    assert "Hello, world!" not in (Migration.migrations_dir / "0001_test_migration.sql").read_text()


@pytest.mark.unit
def test_cmd_new_with_existing_migration_overwrite(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)

    # Pre-create the migration file
    path = Migration.migrations_dir / "0001_test.sql"
    path.write_text("Hello, world!\n")

    with patch("migrateit.models.migration_manager.inquirer.confirm", return_value=True):
        cmd_new(client=mock_client, name="test_migration", no_edit=True)

    assert not (Migration.migrations_dir / "0001_test.sql").exists()
    assert (Migration.migrations_dir / "0001_test_migration.sql").exists()


@pytest.mark.unit
def test_cmd_new_with_dependencies(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)

    cmd_new(client=mock_client, name="test_table", no_edit=True)
    cmd_new(client=mock_client, name="test_table2", dependencies=("0000", "0001"), no_edit=True)

    assert (Migration.migrations_dir / "0001_test_table.sql").exists()

    changelog = ChangelogRepository.load()
    assert len(changelog.migrations) == 3
    assert changelog.migrations[2].name == "0002_test_table2.sql"
    assert changelog.migrations[2].parents == ["0000_migrateit.sql", "0001_test_table.sql"]


@pytest.mark.unit
def test_cmd_new_interactive_with_dependencies_raises_value_error(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    returncode = cmd_new(
        client=mock_client,
        name="test_migration",
        dependencies=("0000_migrateit.sql",),
        interactive=True,
        no_edit=True,
    )
    assert returncode != 0


@pytest.mark.unit
def test_cmd_new_interactive_selection(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)

    prompt_result = {"dependencies": ["0000_migrateit.sql"]}

    with patch("migrateit.cmd.inquirer.prompt", return_value=prompt_result) as mock_prompt:
        cmd_new(client=mock_client, name="interactive_migration", interactive=True, no_edit=True)

        mock_prompt.assert_called_once()
        # Verify choices were generated from existing migrations
        checkbox = mock_prompt.call_args[0][0][0]
        assert checkbox.choices == ["0000_migrateit.sql"]

    changelog = ChangelogRepository.load()
    new_migration = changelog.migrations[-1]
    assert new_migration.name == "0001_interactive_migration.sql"
    assert "0000_migrateit.sql" in new_migration.parents


@pytest.mark.unit
def test_cmd_new_interactive_cancelled(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)

    with patch("migrateit.cmd.inquirer.prompt", return_value=None) as mock_prompt:
        result = cmd_new(client=mock_client, name="cancelled_migration", interactive=True, no_edit=True)

        assert result == 0
        mock_prompt.assert_called_once()

    changelog = ChangelogRepository.load()
    assert changelog.migrations[-1].name == "0001_cancelled_migration.sql"


@pytest.mark.unit
def test_validate_editor_known_safe() -> None:
    for editor in ("vim", "nano", "code", "subl", "emacs", "atom", "zed", "vi"):
        result = _validate_editor(editor)
        assert result == editor


@pytest.mark.unit
def test_validate_editor_absolute_path_valid() -> None:
    # /bin/sh is a valid executable
    result = _validate_editor("/bin/sh")
    assert result == "/bin/sh"


@pytest.mark.unit
def test_validate_editor_unknown_raises() -> None:

    with pytest.raises(ValueError, match="Unsafe editor"):
        _validate_editor("malicious-editor")

    with pytest.raises(ValueError, match="Unsafe editor"):
        _validate_editor("/tmp/nonexistent-editor")


@pytest.mark.unit
def test_cmd_new_no_migrations_table(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    mock_client.is_migrations_table_created.return_value = False  # type: ignore
    assert cmd_new(mock_client, name="test") != 0
