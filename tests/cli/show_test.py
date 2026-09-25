from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from migrateit.cli import cmd_show
from migrateit.clients import SqlClient
from migrateit.models.changelog import Migration
from migrateit.models.migration import MigrationStatus


@pytest.mark.integration
def test_cmd_show_list_mode(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    cmd_client.changelog.migrations = [
        Migration(name="0001_init.sql", initial=True, parents=[]),
        Migration(name="0002_test.sql", parents=["0001_init.sql"]),
    ]

    statuses: dict[str, MigrationStatus] = {
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_test.sql": MigrationStatus.NOT_APPLIED,
    }
    with (
        patch.object(cmd_client, "retrieve_migration_statuses", return_value=statuses),
        patch.object(cmd_client.changelog, "print_list") as mock_print_list,
    ):
        cmd_show(cmd_client, list_mode=True)
        mock_print_list.assert_called_once()


@pytest.mark.integration
def test_cmd_show_dag_mode(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    cmd_client.changelog.migrations = [
        Migration(name="0001_init.sql", initial=True, parents=[]),
        Migration(name="0002_child.sql", parents=["0001_init.sql"]),
    ]

    statuses: dict[str, MigrationStatus] = {
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_child.sql": MigrationStatus.NOT_APPLIED,
    }
    with (
        patch.object(cmd_client, "retrieve_migration_statuses", return_value=statuses),
        patch.object(cmd_client.changelog, "print_dag") as mock_print_dag,
    ):
        cmd_show(cmd_client, list_mode=False)
        mock_print_dag.assert_called_once()


@pytest.mark.integration
def test_cmd_show_validate_sql_success(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    cmd_client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=[])]

    statuses: dict[str, MigrationStatus] = {"0001_init.sql": MigrationStatus.APPLIED}
    with (
        patch.object(cmd_client, "retrieve_migration_statuses", return_value=statuses),
        patch.object(cmd_client, "validate_sql_syntax", return_value=None),
        patch("migrateit.cli.write_line") as mock_write,
    ):
        cmd_show(cmd_client, validate_sql=True)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        sql_validation = [c for c in call_args if "SQL validation" in c]
        assert len(sql_validation) == 1
        assert "passed" in sql_validation[0]


@pytest.mark.integration
def test_cmd_show_validate_sql_failure(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    cmd_client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=[])]

    statuses: dict[str, MigrationStatus] = {"0001_init.sql": MigrationStatus.APPLIED}
    with (
        patch.object(cmd_client, "retrieve_migration_statuses", return_value=statuses),
        patch.object(
            cmd_client,
            "validate_sql_syntax",
            return_value=(Exception("syntax error"), "SELECT *;"),
        ),
        patch("migrateit.cli.write_line") as mock_write,
    ):
        cmd_show(cmd_client, validate_sql=True)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        sql_validation = [c for c in call_args if "SQL validation" in c]
        assert len(sql_validation) == 1
        assert "failed" in sql_validation[0]


@pytest.mark.integration
def test_cmd_show_shows_pending_hint(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    cmd_client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=[])]

    statuses: dict[str, MigrationStatus] = {"0001_init.sql": MigrationStatus.NOT_APPLIED}
    with (
        patch.object(cmd_client, "retrieve_migration_statuses", return_value=statuses),
        patch("migrateit.cli.write_line") as mock_write,
    ):
        cmd_show(cmd_client)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        hints = [c for c in call_args if "pending" in c.lower()]
        assert len(hints) == 1
        assert "migrateit migrate" in hints[0]


@pytest.mark.integration
def test_cmd_show_shows_conflict_hint(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    cmd_client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=[])]

    statuses: dict[str, MigrationStatus] = {"0001_init.sql": MigrationStatus.CONFLICT}
    with (
        patch.object(cmd_client, "retrieve_migration_statuses", return_value=statuses),
        patch("migrateit.cli.write_line") as mock_write,
    ):
        cmd_show(cmd_client)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        hints = [c for c in call_args if "hash conflicts" in c.lower()]
        assert len(hints) == 1


@pytest.mark.integration
def test_cmd_show_shows_removed_hint(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    cmd_client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=[])]

    statuses: dict[str, MigrationStatus] = {
        "0001_init.sql": MigrationStatus.APPLIED,
        "ghost.sql": MigrationStatus.REMOVED,
    }
    with (
        patch.object(cmd_client, "retrieve_migration_statuses", return_value=statuses),
        patch("migrateit.cli.write_line") as mock_write,
    ):
        cmd_show(cmd_client)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        hints = [c for c in call_args if "missing from changelog" in c.lower()]
        assert len(hints) == 1


@pytest.mark.integration
def test_cmd_show_no_hint_when_all_clean(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    cmd_client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=[])]

    statuses: dict[str, MigrationStatus] = {"0001_init.sql": MigrationStatus.APPLIED}
    with (
        patch.object(cmd_client, "retrieve_migration_statuses", return_value=statuses),
        patch("migrateit.cli.write_line") as mock_write,
    ):
        cmd_show(cmd_client)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        hints = [c for c in call_args if "pending" in c.lower() or "conflicts" in c.lower() or "missing" in c.lower()]
        assert len(hints) == 0
