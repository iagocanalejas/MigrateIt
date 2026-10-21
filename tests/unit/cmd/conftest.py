from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from migrateit.clients._client import SqlClient
from migrateit.models.config import SupportedDatabase
from migrateit.models.migration import Migration
from migrateit.models.migration_manager import MigrationManager
from migrateit.models.repository import ChangelogRepository
from tests.unit.conftest import INITIAL_MIGRATION


def _mock_client(temp_dir: Path) -> SqlClient[Any]:
    changelog = ChangelogRepository.create(database=SupportedDatabase.POSTGRES)
    changelog.migrations.append(Migration(name=INITIAL_MIGRATION, initial=True, parents=()))
    ChangelogRepository.save(changelog)

    mock_client: MagicMock = MagicMock(spec=SqlClient[Any])
    mock_client.changelog = changelog
    mock_client.migrations = MigrationManager(changelog)
    mock_client.migrations_dir = temp_dir / "migrations"
    mock_client.migrations_dir.mkdir(parents=True, exist_ok=True)
    return mock_client
