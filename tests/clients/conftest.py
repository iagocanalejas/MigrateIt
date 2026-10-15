from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from migrateit.clients._client import SqlClient
from migrateit.clients.sqlite import SqliteClient
from migrateit.models.config import MigrateItConfig, SupportedDatabase
from migrateit.models.migration import Migration
from migrateit.models.repository import ChangelogRepository
from tests.conftest import INITIAL_MIGRATION, TEST_MIGRATIONS_TABLE


def _client_with_mocked_connection(temp_dir: Path) -> SqlClient[Any]:
    changelog = ChangelogRepository.create(temp_dir / "changelog.json", database=SupportedDatabase.POSTGRES)
    changelog.migrations.append(Migration(name=INITIAL_MIGRATION, initial=True, parents=()))
    ChangelogRepository.save(changelog)

    config = MigrateItConfig(
        table_name=TEST_MIGRATIONS_TABLE,
        migrations_dir=temp_dir,
        changelog=ChangelogRepository.load(temp_dir / "changelog.json"),
    )
    return SqliteClient(MagicMock(), config)
