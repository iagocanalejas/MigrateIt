from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from migrateit.clients import PsqlClient
from migrateit.models import ChangelogFile, Migration
from migrateit.models.migration import MigrationStatus
from tests.conftest import TEST_MIGRATIONS_TABLE


def _insert_migration_row(pg_client: PsqlClient, name: str, hash_value: str) -> None:
    with pg_client.connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (%s, %s)",
            (name, hash_value),
        )
    pg_client.connection.commit()


@pytest.mark.postgres
@patch.object(PsqlClient, "get_migration_content_and_hash")
def test_show_migrations_applied_and_not_applied(
    mock_get_migration_content_and_hash: MagicMock,
    pg_client: PsqlClient,
    temp_dir: Path,
) -> None:
    migration_applied = Migration(name="001_init.sql")
    migration_not_applied = Migration(name="002_more.sql")

    mock_get_migration_content_and_hash.return_value = ("dummy_content", "dummy_reverse_content", "hash1")
    _insert_migration_row(pg_client, "001_init.sql", "hash1")

    pg_client.config.changelog = ChangelogFile(version=1, migrations=[migration_applied, migration_not_applied])

    result = pg_client.retrieve_migration_statuses()

    expected = {
        migration_applied.name: MigrationStatus.APPLIED,
        migration_not_applied.name: MigrationStatus.NOT_APPLIED,
    }
    assert result == expected


@pytest.mark.postgres
@patch.object(PsqlClient, "get_migration_content_and_hash")
def test_show_migrations_conflict_and_removed(
    mock_get_migration_content_and_hash: MagicMock,
    pg_client: PsqlClient,
    temp_dir: Path,
) -> None:
    mock_get_migration_content_and_hash.return_value = ("dummy_content", "dummy_reverse_content", "expected_hash")

    _insert_migration_row(pg_client, "001_init.sql", "different_hash")  # mismatch
    _insert_migration_row(pg_client, "ghost.sql", "ghost_hash")

    changelog = ChangelogFile(version=1, migrations=[Migration(name="001_init.sql")])
    pg_client.config.changelog = changelog

    result = pg_client.retrieve_migration_statuses()

    assert result["001_init.sql"] == MigrationStatus.CONFLICT
    assert result["ghost.sql"] == MigrationStatus.REMOVED


@pytest.mark.postgres
@patch.object(PsqlClient, "get_migration_content_and_hash")
def test_show_migrations_order_error(
    mock_get_migration_content_and_hash: MagicMock,
    pg_client: PsqlClient,
    temp_dir: Path,
) -> None:
    mock_get_migration_content_and_hash.side_effect = [
        ("dummy_content", "dummy_reverse_content", "hash2"),  # for 002_second.sql
        ("dummy_content", "dummy_reverse_content", "hash1"),  # for 001_second.sql
    ]
    _insert_migration_row(pg_client, "002_second.sql", "hash2")
    changelog = ChangelogFile(
        version=1,
        migrations=[
            Migration(name="001_first.sql", initial=True, parents=[]),
            Migration(name="002_second.sql", parents=["001_first.sql"]),
        ],
    )
    pg_client.config.changelog = changelog

    statuses = pg_client.retrieve_migration_statuses()
    with pytest.raises(ValueError) as cm:
        pg_client.validate_migrations(statuses)
    assert "is applied before" in str(cm.value)
