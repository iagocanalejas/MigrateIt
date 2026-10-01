from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from migrateit.clients._client import SqlClient
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.migration import Migration
from migrateit.tree import create_changelog_file
from tests.conftest import INITIAL_MIGRATION


def _mock_client(temp_dir: Path) -> SqlClient[Any]:
    changelog = create_changelog_file(temp_dir / "changelog.json", database=SupportedDatabase.POSTGRES)
    changelog.migrations.append(Migration(name=INITIAL_MIGRATION, initial=True, parents=[]))

    mock_client: MagicMock = MagicMock(spec=SqlClient[Any])
    mock_client.changelog = changelog
    mock_client.migrations_dir = temp_dir / "migrations"
    mock_client.migrations_dir.mkdir(parents=True, exist_ok=True)
    return mock_client
