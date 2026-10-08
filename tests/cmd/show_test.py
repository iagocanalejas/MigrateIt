from pathlib import Path
from unittest.mock import patch

import pytest

from migrateit.cmd import cmd_show
from migrateit.models.migration import Migration, MigrationStatus

from .conftest import _mock_client


@pytest.mark.unit
def test_cmd_show_list_mode(temp_dir: Path) -> None:
    client = _mock_client(temp_dir)
    client.retrieve_migration_statuses.return_value = {  # type: ignore
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_test.sql": MigrationStatus.NOT_APPLIED,
    }

    client.changelog.migrations = [
        Migration(name="0001_init.sql", initial=True, parents=()),
        Migration(name="0002_test.sql", parents=("0001_init.sql",)),
    ]

    with patch.object(client.changelog, "print_list") as mock_print_list:
        cmd_show(client, list_mode=True)
        mock_print_list.assert_called_once()


@pytest.mark.unit
def test_cmd_show_dag_mode(temp_dir: Path) -> None:
    client = _mock_client(temp_dir)
    client.retrieve_migration_statuses.return_value = {  # type: ignore
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_child.sql": MigrationStatus.NOT_APPLIED,
    }

    client.changelog.migrations = [
        Migration(name="0001_init.sql", initial=True, parents=()),
        Migration(name="0002_child.sql", parents=("0001_init.sql",)),
    ]

    with patch.object(client.changelog, "print_dag") as mock_print_dag:
        cmd_show(client, list_mode=False)
        mock_print_dag.assert_called_once()


@pytest.mark.unit
def test_cmd_show_validate_sql_success(temp_dir: Path) -> None:
    client = _mock_client(temp_dir)
    client.retrieve_migration_statuses.return_value = {"0001_init.sql": MigrationStatus.APPLIED}  # type: ignore
    client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=())]

    with (
        patch.object(client, "validate_sql_syntax", return_value=None),
        patch("migrateit.cmd.write_line") as mock_write,
    ):
        cmd_show(client, validate_sql=True)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        sql_validation = [c for c in call_args if "SQL validation" in c]
        assert len(sql_validation) == 1
        assert "passed" in sql_validation[0]


@pytest.mark.unit
def test_cmd_show_validate_sql_failure(temp_dir: Path) -> None:
    client = _mock_client(temp_dir)
    client.retrieve_migration_statuses.return_value = {"0001_init.sql": MigrationStatus.APPLIED}  # type: ignore
    client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=())]

    with (
        patch.object(client, "validate_sql_syntax", return_value=(Exception("syntax error"), "SELECT *;")),
        patch("migrateit.cmd.write_line") as mock_write,
    ):
        cmd_show(client, validate_sql=True)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        sql_validation = [c for c in call_args if "SQL validation" in c]
        assert len(sql_validation) == 1
        assert "failed" in sql_validation[0]


@pytest.mark.unit
def test_cmd_show_shows_pending_hint(temp_dir: Path) -> None:
    client = _mock_client(temp_dir)
    client.retrieve_migration_statuses.return_value = {"0001_init.sql": MigrationStatus.NOT_APPLIED}  # type: ignore
    client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=())]

    with patch("migrateit.cmd.write_line") as mock_write:
        cmd_show(client)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        hints = [c for c in call_args if "pending" in c.lower()]
        assert len(hints) == 1
        assert "migrateit migrate" in hints[0]


@pytest.mark.unit
@pytest.mark.parametrize(
    "statuses,search_term",
    [
        pytest.param(
            {"0001_init.sql": MigrationStatus.CONFLICT},
            "hash conflicts",
            id="conflict",
        ),
        pytest.param(
            {"0001_init.sql": MigrationStatus.APPLIED, "ghost.sql": MigrationStatus.REMOVED},
            "missing from changelog",
            id="removed",
        ),
    ],
)
def test_cmd_show_shows_hint(temp_dir: Path, statuses: dict[str, MigrationStatus], search_term: str) -> None:
    client = _mock_client(temp_dir)
    client.retrieve_migration_statuses.return_value = statuses  # type: ignore
    client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=())]

    with patch("migrateit.cmd.write_line") as mock_write:
        cmd_show(client)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        hints = [c for c in call_args if search_term in c.lower()]
        assert len(hints) == 1


@pytest.mark.unit
def test_cmd_show_no_hint_when_all_clean(temp_dir: Path) -> None:
    client = _mock_client(temp_dir)
    client.retrieve_migration_statuses.return_value = {"0001_init.sql": MigrationStatus.APPLIED}  # type: ignore
    client.changelog.migrations = [Migration(name="0001_init.sql", initial=True, parents=())]

    with patch("migrateit.cmd.write_line") as mock_write:
        cmd_show(client)
        call_args = [c[0][0] for c in mock_write.call_args_list]
        hints = [c for c in call_args if "pending" in c.lower() or "conflicts" in c.lower() or "missing" in c.lower()]
        assert len(hints) == 0
