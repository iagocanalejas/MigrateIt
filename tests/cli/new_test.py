import os
from pathlib import Path
from typing import Any

import pytest

from migrateit.cli import cmd_new
from migrateit.clients import SqlClient
from migrateit.tree import load_changelog_file


@pytest.mark.integration
def test_cmd_new(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test cmd_new creates a migration file and updates the changelog."""
    client = cmd_client
    cmd_new(client=client, name="test_migration", no_edit=True)

    assert os.path.exists(client.migrations_dir / "0001_test_migration.sql")

    changelog = load_changelog_file(client.changelog.path)
    assert len(changelog.migrations) == 2
    assert changelog.migrations[1].name == "0001_test_migration.sql"


@pytest.mark.integration
def test_cmd_new_with_existing_migration(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test cmd_new raises FileExistsError when migration file already exists."""
    client = cmd_client

    # Pre-create the migration file
    path = client.migrations_dir / "0001_test_migration.sql"
    path.write_text("Hello, world!\n")

    with pytest.raises(FileExistsError) as ctx:
        cmd_new(client=client, name="test_migration", no_edit=True)

    assert "already exists" in str(ctx.value)


@pytest.mark.integration
def test_cmd_new_with_dependencies(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test cmd_new creates migrations with explicit dependencies."""
    client = cmd_client

    cmd_new(client=client, name="test_migration", no_edit=True)
    cmd_new(client=client, name="test_migration", dependencies=["0000", "0001"], no_edit=True)

    assert os.path.exists(client.migrations_dir / "0002_test_migration.sql")

    changelog = load_changelog_file(client.changelog.path)
    assert len(changelog.migrations) == 3
    assert changelog.migrations[2].name == "0002_test_migration.sql"
    assert changelog.migrations[2].parents == ["0000_migrateit.sql", "0001_test_migration.sql"]
