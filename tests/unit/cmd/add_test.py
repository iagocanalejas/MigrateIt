from pathlib import Path
from unittest.mock import patch

import pytest

from migrateit.cmd.add import cmd_add
from migrateit.models.repository import ChangelogRepository
from tests.unit.conftest import MIGRATION_NAME, _create_migration_file

from .conftest import _mock_client


@pytest.mark.unit
def test_cmd_add(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    migration = _create_migration_file(MIGRATION_NAME)

    result = cmd_add(mock_client, name=migration.name, dependencies=("0000",))
    assert result == 0

    changelog = ChangelogRepository.load()
    new_migration = changelog.migrations[-1]
    assert new_migration.name == MIGRATION_NAME
    assert "0000_migrateit.sql" in new_migration.parents


@pytest.mark.unit
def test_cmd_add_interactive_selection(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    migration = _create_migration_file(MIGRATION_NAME)

    prompt_result = {"dependencies": ["0000_migrateit.sql"]}
    with patch("migrateit.cmd.add.inquirer.prompt", return_value=prompt_result) as mock_prompt:
        result = cmd_add(client=mock_client, name=migration.name, interactive=True)

        assert result == 0
        mock_prompt.assert_called_once()
        # Verify choices were generated from existing migrations
        checkbox = mock_prompt.call_args[0][0][0]
        assert checkbox.choices == ["0000_migrateit.sql"]

    changelog = ChangelogRepository.load()
    new_migration = changelog.migrations[-1]
    assert new_migration.name == MIGRATION_NAME
    assert "0000_migrateit.sql" in new_migration.parents


@pytest.mark.unit
def test_cmd_add_interactive_cancelled(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    migration = _create_migration_file(MIGRATION_NAME)

    with patch("migrateit.cmd.new.inquirer.prompt", return_value=None) as mock_prompt:
        result = cmd_add(client=mock_client, name=migration.name, interactive=True)

        assert result == 1
        mock_prompt.assert_called_once()


@pytest.mark.unit
def test_cmd_add_no_file(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    assert cmd_add(mock_client, name="no_name") != 0


@pytest.mark.unit
def test_cmd_add_multiple_file(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    _create_migration_file("0001_migration.sql")
    _create_migration_file("0001_migration2.sql")
    assert cmd_add(mock_client, name="0001") != 0


@pytest.mark.unit
def test_cmd_add_no_dependencies(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    migration = _create_migration_file(MIGRATION_NAME)
    assert cmd_add(mock_client, name=migration.name) != 0


@pytest.mark.unit
def test_cmd_add_non_existing_dependencies(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    migration = _create_migration_file(MIGRATION_NAME)
    assert cmd_add(mock_client, name=migration.name, dependencies=("0003",)) != 0


@pytest.mark.unit
def test_cmd_add_interactive_with_dependencies(temp_dir: Path) -> None:
    mock_client = _mock_client(temp_dir)
    migration = _create_migration_file(MIGRATION_NAME)
    assert cmd_add(mock_client, name=migration.name, dependencies=("0000",), interactive=True) != 0
