import os
from unittest.mock import MagicMock, patch

import pytest

from migrateit import cli
from migrateit.clients.psql import PsqlClient
from migrateit.models import Migration
from migrateit.models.migration import MigrationStatus


def test_cmd_new_raises_when_table_not_created() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    mock_client.is_migrations_table_created.return_value = False

    with pytest.raises(ValueError) as ctx:
        cli.cmd_new(mock_client, name="test_migration", no_edit=True)
    assert "does not exist" in str(ctx.value)


def test_cmd_run_hash_update_success() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    mock_client.connection = MagicMock()
    mock_target = MagicMock(spec=Migration)
    mock_target.name = "0001_test.sql"
    mock_target.initial = False
    mock_client.changelog.get_migration_by_name.return_value = mock_target

    result = cli.cmd_run(mock_client, name="0001_test.sql", is_hash_update=True)

    mock_client.update_migration_hash.assert_called_once_with(mock_target)
    mock_client.connection.commit.assert_called_once()
    assert result == 0


def test_cmd_run_hash_update_no_target() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    with pytest.raises(ValueError) as ctx:
        cli.cmd_run(mock_client, name=None, is_hash_update=True)
    assert "requires a target migration name" in str(ctx.value)


def test_cmd_run_hash_update_initial_raises() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    mock_target = MagicMock(spec=Migration)
    mock_target.name = "0000_init.sql"
    mock_target.initial = True
    mock_client.changelog.get_migration_by_name.return_value = mock_target

    with pytest.raises(ValueError) as ctx:
        cli.cmd_run(mock_client, name="0000", is_hash_update=True)
    assert "Cannot update hash for the initial migration" in str(ctx.value)


def test_cmd_run_fake_no_target() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    mock_client.is_migrations_table_created.return_value = True
    mock_client.retrieve_migration_statuses.return_value = {}

    with pytest.raises(ValueError) as ctx:
        cli.cmd_run(mock_client, name=None, is_fake=True)
    assert "Fake migration requires a target migration name" in str(ctx.value)


def test_cmd_run_fake_initial_raises() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    mock_target = MagicMock(spec=Migration)
    mock_target.name = "0000_init.sql"
    mock_target.initial = True
    mock_client.changelog.get_migration_by_name.return_value = mock_target
    mock_client.retrieve_migration_statuses.return_value = {}

    with pytest.raises(ValueError) as ctx:
        cli.cmd_run(mock_client, name="0000", is_fake=True)
    assert "Cannot fake the initial migration" in str(ctx.value)


def test_cmd_run_rollback_no_target() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    mock_client.is_migrations_table_created.return_value = True
    mock_client.retrieve_migration_statuses.return_value = {}

    with pytest.raises(ValueError) as ctx:
        cli.cmd_run(mock_client, name=None, is_rollback=True)
    assert "Rollback requires a target migration name" in str(ctx.value)


def test_cmd_show_calls_print_dag() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    m2 = Migration(name="0002_add.sql", parents=["0001_init.sql"])
    tree: dict[str, list[Migration]] = {
        "0001_init.sql": [m2],
        "0002_add.sql": [],
    }
    mock_client.retrieve_migration_statuses.return_value = {
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_add.sql": MigrationStatus.NOT_APPLIED,
    }
    with patch("migrateit.cli.build_migrations_tree", return_value=tree):
        with patch("migrateit.cli.print_dag") as mock_print_dag:
            cli.cmd_show(mock_client, list_mode=False)
            mock_print_dag.assert_called_once()


def test_cmd_show_calls_print_list() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    tree: dict[str, list[Migration]] = {"0001_init.sql": []}
    mock_client.retrieve_migration_statuses.return_value = {
        "0001_init.sql": MigrationStatus.APPLIED,
    }
    with patch("migrateit.cli.build_migrations_tree", return_value=tree):
        with patch("migrateit.cli.print_list") as mock_print_list:
            cli.cmd_show(mock_client, list_mode=True)
            mock_print_list.assert_called_once()



def test_cmd_show_validate_sql_success() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    m1 = Migration(name="0001_init.sql", initial=True, parents=[])
    tree: dict[str, list[Migration]] = {"0001_init.sql": []}
    mock_client.retrieve_migration_statuses.return_value = {
        "0001_init.sql": MigrationStatus.APPLIED,
    }
    mock_client.changelog.migrations = [m1]
    mock_client.validate_sql_syntax.return_value = None
    with patch("migrateit.cli.build_migrations_tree", return_value=tree):
        with patch("migrateit.cli.write_line") as mock_write:
            cli.cmd_show(mock_client, validate_sql=True)
            call_args = [c[0][0] for c in mock_write.call_args_list]
            sql_validation = [c for c in call_args if "SQL validation" in c]
            assert len(sql_validation) == 1
            assert "passed" in sql_validation[0]


def test_cmd_show_validate_sql_failure() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    m1 = Migration(name="0001_init.sql", initial=True, parents=[])
    tree: dict[str, list[Migration]] = {"0001_init.sql": []}
    mock_client.retrieve_migration_statuses.return_value = {
        "0001_init.sql": MigrationStatus.APPLIED,
    }
    mock_client.changelog.migrations = [m1]
    mock_client.validate_sql_syntax.return_value = (Exception("syntax error"), "SELECT *;")
    with patch("migrateit.cli.build_migrations_tree", return_value=tree):
        with patch("migrateit.cli.write_line") as mock_write:
            cli.cmd_show(mock_client, validate_sql=True)
            call_args = [c[0][0] for c in mock_write.call_args_list]
            sql_validation = [c for c in call_args if "SQL validation" in c]
            assert len(sql_validation) == 1
            assert "failed" in sql_validation[0]


def test_cmd_squash_no_end_migration() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    m1 = Migration(name="0000_init.sql", initial=True)
    m2 = Migration(name="0001_second.sql", parents=["0000_init.sql"])
    mock_client.changelog.migrations = [m1, m2]
    mock_client.changelog.get_migration_by_name = lambda n: m2 if "second" in n else m1

    with patch("migrateit.cli.build_migrations_tree", return_value={}):
        with patch("migrateit.cli.find_path", return_value=[]):
            with pytest.raises(ValueError) as ctx:
                cli.cmd_squash(mock_client, start_migration="0001")
            assert "No path found" in str(ctx.value)


def test_cmd_squash_initial_migration() -> None:
    mock_client = MagicMock(spec=PsqlClient)
    m1 = Migration(name="0000_init.sql", initial=True)
    m2 = Migration(name="0001_second.sql", parents=["0000_init.sql"])
    mock_client.changelog.migrations = [m1, m2]
    mock_client.changelog.get_migration_by_name = lambda n: m1 if "init" in n else m2

    with patch("migrateit.cli.build_migrations_tree", return_value={}):
        with patch("migrateit.cli.find_path", return_value=["0000_init.sql"]):
            with pytest.raises(ValueError) as ctx:
                cli.cmd_squash(mock_client, start_migration="0000", end_migration="0000")
            assert "Cannot squash initial migrations" in str(ctx.value)


def test_get_environment_url_from_db_url() -> None:
    with patch.dict(os.environ, {"DB_URL": "postgresql://user:pass@host:5432/mydb"}):
        url = PsqlClient.get_environment_url()
        assert url == "postgresql://user:pass@host:5432/mydb"


def test_get_environment_url_builds_from_parts() -> None:
    env: dict[str, str] = {
        "DB_HOST": "testhost",
        "DB_PORT": "5433",
        "DB_USER": "testuser",
        "DB_PASS": "testpass",
        "DB_NAME": "testdb",
    }
    with patch.dict(os.environ, env):
        url = PsqlClient.get_environment_url()
        assert "testhost" in url
        assert "5433" in url


def test_get_environment_url_no_password() -> None:
    env: dict[str, str] = {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_USER": "postgres",
        "DB_NAME": "migrateit",
    }
    with patch.dict(os.environ, env):
        url = PsqlClient.get_environment_url()
        assert ":@" not in url


def test_create_migrations_table_str_valid() -> None:
    sql, rollback = PsqlClient.create_migrations_table_str("my_migrations")
    assert "CREATE TABLE" in sql
    assert "my_migrations" in sql
    assert "DROP TABLE" in rollback


def test_create_migrations_table_str_invalid() -> None:
    with pytest.raises(ValueError):
        PsqlClient.create_migrations_table_str("invalid-table-name")


def test_create_migrations_table_str_number() -> None:
    with pytest.raises(ValueError):
        PsqlClient.create_migrations_table_str("123")
