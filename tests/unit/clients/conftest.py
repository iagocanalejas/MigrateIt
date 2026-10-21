from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from migrateit.clients._client import SqlClient
from migrateit.clients.sqlite import SqliteClient
from migrateit.models.config import MigrateItConfig, SupportedDatabase
from migrateit.models.migration import Migration
from migrateit.models.repository import ChangelogRepository
from tests.unit.conftest import INITIAL_MIGRATION


def _client_with_mocked_connection(temp_dir: Path) -> SqlClient[Any]:
    changelog = ChangelogRepository.create(database=SupportedDatabase.POSTGRES)
    changelog.migrations.append(Migration(name=INITIAL_MIGRATION, initial=True, parents=()))
    ChangelogRepository.save(changelog)

    config = MigrateItConfig(changelog=ChangelogRepository.load())
    return SqliteClient(MagicMock(), config)
