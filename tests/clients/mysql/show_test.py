from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from migrateit.clients.mysql import MySqlClient
from migrateit.models import ChangelogFile, Migration
from migrateit.models.migration import MigrationStatus
from tests.conftest import TEST_MIGRATIONS_TABLE


def _insert_migration_row(mysql_client: MySqlClient, name: str, hash_value: str) -> None:
    with mysql_client.connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (%s, %s)",
            (name, hash_value),
        )
    mysql_client.connection.commit()


@pytest.mark.mysql
@patch.object(MySqlClient, "get_migration_content_and_hash")
def test_show_migrations_applied_and_not_applied(
    mock_get_hash: MagicMock,
    mysql_client: MySqlClient,
    temp_dir: Path,
) -> None:
    """Test retrieving migration statuses with applied and not-applied migrations."""
    migration_applied = Migration(name="001_init.sql")
    migration_not_applied = Migration(name="002_more.sql")

    mock_get_hash.return_value = ("dummy", "dummy", "hash1")
    _insert_migration_row(mysql_client, "001_init.sql", "hash1")

    mysql_client.config.changelog = ChangelogFile(version=1, migrations=[migration_applied, migration_not_applied])

    result = mysql_client.retrieve_migration_statuses()

    expected = {
        migration_applied.name: MigrationStatus.APPLIED,
        migration_not_applied.name: MigrationStatus.NOT_APPLIED,
    }
    assert result == expected


@pytest.mark.mysql
@patch.object(MySqlClient, "get_migration_content_and_hash")
def test_show_migrations_conflict_and_removed(
    mock_get_hash: MagicMock,
    mysql_client: MySqlClient,
    temp_dir: Path,
) -> None:
    """Test conflict (hash mismatch) and removed (ghost) migration detection."""
    mock_get_hash.return_value = ("dummy_content", "dummy_reverse_content", "expected_hash")

    _insert_migration_row(mysql_client, "001_init.sql", "different_hash")  # mismatch
    _insert_migration_row(mysql_client, "ghost.sql", "ghost_hash")

    changelog = ChangelogFile(version=1, migrations=[Migration(name="001_init.sql")])
    mysql_client.config.changelog = changelog

    result = mysql_client.retrieve_migration_statuses()

    assert result["001_init.sql"] == MigrationStatus.CONFLICT
    assert result["ghost.sql"] == MigrationStatus.REMOVED


@pytest.mark.mysql
def test_show_migrations_order_error(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test validate_migrations raises when child is applied before parent."""
    changelog = ChangelogFile(
        version=1,
        migrations=[
            Migration(name="0000_migrateit.sql", initial=True, parents=[]),
            Migration(name="0001_first.sql", parents=["0000_migrateit.sql"]),
            Migration(name="0002_second.sql", parents=["0001_first.sql"]),
        ],
    )
    mysql_client.config.changelog = changelog

    # Force a status map where 0002 is APPLIED but its parent 0001 is NOT
    statuses = {
        "0000_migrateit.sql": MigrationStatus.APPLIED,
        "0001_first.sql": MigrationStatus.NOT_APPLIED,
        "0002_second.sql": MigrationStatus.APPLIED,
    }
    with pytest.raises(ValueError) as cm:
        mysql_client.validate_migrations(statuses)
    assert "is applied before its parent" in str(cm.value)


@pytest.mark.mysql
def test_no_table_returns_not_applied(mysql_client: MySqlClient, temp_dir: Path) -> None:
    """Test retrieving statuses when no migrations table exists."""
    statuses = mysql_client.retrieve_migration_statuses()
    assert len(statuses) == 1
    assert statuses["0000_migrateit.sql"] == MigrationStatus.NOT_APPLIED
