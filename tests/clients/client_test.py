from pathlib import Path
from unittest.mock import MagicMock

import pytest

from migrateit.clients._client import SqlClient
from migrateit.models import ChangelogFile, MigrateItConfig
from migrateit.tree import ROLLBACK_SPLIT_TAG


@pytest.mark.unit
def test_sql_client_none_connection_raises(temp_dir: Path) -> None:
    config = MigrateItConfig(
        table_name="migrations",
        migrations_dir=temp_dir,
        changelog=ChangelogFile(version=1, path=temp_dir / "changelog.json"),
    )
    with pytest.raises(ValueError, match="connection cannot be None"):
        SqlClient[None](None, config)  # type: ignore[abstract]


@pytest.mark.unit
def test_sql_client_valid_config(temp_dir: Path) -> None:
    config = MigrateItConfig(
        table_name="migrations",
        migrations_dir=temp_dir,
        changelog=ChangelogFile(version=1, path=temp_dir / "changelog.json"),
    )
    mock_conn = MagicMock()
    client: SqlClient[MagicMock] = SqlClient(mock_conn, config)  # type: ignore[abstract]
    assert client.config == config
    assert client.connection == mock_conn
    assert client.table_name == "migrations"
    assert client.migrations_dir == temp_dir
    assert isinstance(client.changelog, ChangelogFile)


@pytest.mark.unit
def test_validate_config_empty_table_name() -> None:
    config = MigrateItConfig(
        table_name="",
        migrations_dir=Path("/tmp/migrations"),
        changelog=ChangelogFile(version=1, path=Path("/tmp/changelog.json")),
    )
    with pytest.raises(ValueError, match="Table name is required"):
        SqlClient.validate_config(config)


@pytest.mark.unit
def test_validate_config_non_string_table_name() -> None:
    config = MigrateItConfig(
        table_name=int(123),  # type: ignore[arg-type]
        migrations_dir=Path("/tmp/migrations"),
        changelog=ChangelogFile(version=1, path=Path("/tmp/changelog.json")),
    )
    with pytest.raises(TypeError):
        SqlClient.validate_config(config)


@pytest.mark.unit
def test_validate_config_invalid_identifier() -> None:
    config = MigrateItConfig(
        table_name="invalid-name",
        migrations_dir=Path("/tmp/migrations"),
        changelog=ChangelogFile(version=1, path=Path("/tmp/changelog.json")),
    )
    with pytest.raises(ValueError, match="valid identifier"):
        SqlClient.validate_config(config)


@pytest.mark.unit
def test_get_migration_content_and_hash_no_rollback(temp_dir: Path) -> None:
    filename = "0007_missing_reverse.sql"
    migrations_dir = temp_dir / "migrations"

    migrations_dir.mkdir(parents=True, exist_ok=True)
    with open(migrations_dir / filename, "w") as f:
        f.write("SELECT 1;")

    with pytest.raises(ValueError, match="No rollback"):
        SqlClient.get_migration_content_and_hash(migrations_dir / filename)


@pytest.mark.unit
def test_get_migration_content_and_hash_more_than_one_rollback(temp_dir: Path) -> None:
    filename = "0007_missing_reverse.sql"
    migrations_dir = temp_dir / "migrations"

    migrations_dir.mkdir(parents=True, exist_ok=True)
    with open(migrations_dir / filename, "w") as f:
        f.write("SELECT 1;" + ROLLBACK_SPLIT_TAG + "SELECT 2;" + ROLLBACK_SPLIT_TAG + "SELECT 3;")

    with pytest.raises(ValueError, match="Too many rollback"):
        SqlClient.get_migration_content_and_hash(migrations_dir / filename)
