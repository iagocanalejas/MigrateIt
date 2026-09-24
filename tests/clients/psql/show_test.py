from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from migrateit.clients import PsqlClient
from migrateit.models import ChangelogFile, Migration
from migrateit.models.migration import MigrationStatus
from tests.conftest import TEST_MIGRATIONS_TABLE


def _insert_migration_row(client: PsqlClient, name: str, hash_value: str) -> None:
    with client.connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (%s, %s)",
            (name, hash_value),
        )
    client.connection.commit()


def _create_migration_table(client: PsqlClient) -> None:
    # Create the migrations table first
    sql, _ = PsqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
    client.connection.commit()


@patch.object(PsqlClient, "_get_migration_content_and_hash")
def test_show_migrations_applied_and_not_applied(
    mock_get_migration_content_and_hash: MagicMock,
    client: PsqlClient,
    temp_dir: Path,
) -> None:
    _create_migration_table(client)
    migration_applied = Migration(name="001_init.sql")
    migration_not_applied = Migration(name="002_more.sql")

    mock_get_migration_content_and_hash.return_value = ("dummy_content", "dummy_reverse_content", "hash1")
    _insert_migration_row(client, "001_init.sql", "hash1")

    changelog = ChangelogFile(version=1, migrations=[migration_applied, migration_not_applied])
    client.config.changelog = changelog

    result = client.retrieve_migration_statuses()

    expected = {
        migration_applied.name: MigrationStatus.APPLIED,
        migration_not_applied.name: MigrationStatus.NOT_APPLIED,
    }
    assert result == expected


@patch.object(PsqlClient, "_get_migration_content_and_hash")
def test_show_migrations_conflict_and_removed(
    mock_get_migration_content_and_hash: MagicMock,
    client: PsqlClient,
    temp_dir: Path,
) -> None:
    _create_migration_table(client)

    mock_get_migration_content_and_hash.return_value = ("dummy_content", "dummy_reverse_content", "expected_hash")

    _insert_migration_row(client, "001_init.sql", "different_hash")  # mismatch
    _insert_migration_row(client, "ghost.sql", "ghost_hash")

    changelog = ChangelogFile(version=1, migrations=[Migration(name="001_init.sql")])
    client.config.changelog = changelog

    result = client.retrieve_migration_statuses()

    assert result["001_init.sql"] == MigrationStatus.CONFLICT
    assert result["ghost.sql"] == MigrationStatus.REMOVED


@patch.object(PsqlClient, "_get_migration_content_and_hash")
def test_show_migrations_order_error(
    mock_get_migration_content_and_hash: MagicMock,
    client: PsqlClient,
    temp_dir: Path,
) -> None:
    _create_migration_table(client)

    mock_get_migration_content_and_hash.side_effect = [
        ("dummy_content", "dummy_reverse_content", "hash2"),  # for 002_second.sql
        ("dummy_content", "dummy_reverse_content", "hash1"),  # for 001_second.sql
    ]
    _insert_migration_row(client, "002_second.sql", "hash2")
    changelog = ChangelogFile(
        version=1,
        migrations=[
            Migration(name="001_first.sql", initial=True, parents=[]),
            Migration(name="002_second.sql", parents=["001_first.sql"]),
        ],
    )
    client.config.changelog = changelog

    statuses = client.retrieve_migration_statuses()
    with pytest.raises(ValueError) as cm:
        client.validate_migrations(statuses)
    assert "is applied before" in str(cm.value)
