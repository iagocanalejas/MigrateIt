import json
import sqlite3
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from migrateit import constants as C
from migrateit.main import main
from migrateit.models.changelog import SupportedDatabase


def _resolve_database(client_class_name: str) -> SupportedDatabase:
    """Derive database type from a client class name."""
    if client_class_name == "PsqlClient":
        return SupportedDatabase.POSTGRES
    if client_class_name == "MySqlClient":
        return SupportedDatabase.MYSQL
    if client_class_name == "SqliteClient":
        return SupportedDatabase.SQLITE
    raise NotImplementedError  # pragma: no cover


def _load_changelog(
    temp_dir: Path,
    database: SupportedDatabase = SupportedDatabase.SQLITE,
    migrations: list[dict[str, object]] | None = None,
) -> Path:
    """Write a minimal changelog.json into temp_dir and return its path."""
    changelog_path = temp_dir / "changelog.json"
    changelog_path.write_text(
        json.dumps(
            {
                "version": 1,
                "database": database.value,
                "migrations": migrations or [{"name": "0000_init.sql", "parents": [], "initial": True}],
            }
        )
    )
    return changelog_path


@pytest.fixture(autouse=True)
def patch_migrateit_root(temp_dir: Path) -> Generator[Path]:
    (temp_dir / "migrations").mkdir(parents=True, exist_ok=True)
    with patch("migrateit.constants.MIGRATEIT_ROOT_DIR", str(temp_dir)):
        yield temp_dir


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_main_init(temp_dir: Path, database: SupportedDatabase) -> None:
    """Test main() dispatches the init command for every supported database."""
    with (
        patch("migrateit.main.commands.cmd_init", return_value=0) as mock_init,
        patch("sys.argv", ["migrateit", "init", database.value]),
    ):
        result = main()

    assert result == 0
    mock_init.assert_called_once_with(
        table_name=C.MIGRATEIT_MIGRATIONS_TABLE,
        migrations_dir=temp_dir / "migrations",
        migrations_file=temp_dir / "changelog.json",
        database=database,
    )


@pytest.mark.unit
def test_main_new(temp_dir: Path, mock_conn_and_client: tuple[MagicMock | sqlite3.Connection, str]) -> None:
    """Test main() dispatches the ``new`` command for every database."""
    db_conn, client_class_name = mock_conn_and_client
    db = _resolve_database(client_class_name)
    _load_changelog(
        temp_dir,
        database=db,
        migrations=[{"name": "0000_init.sql", "parents": [], "initial": True}],
    )
    with (
        patch("migrateit.main.get_connection", return_value=db_conn),
        patch(f"migrateit.main.{client_class_name}"),
        patch("migrateit.main.commands.cmd_new", return_value=0),
        patch("sys.argv", ["migrateit", "new", "add_users_table", "-d", "0000_init.sql"]),
    ):
        result = main()

    assert result == 0


@pytest.mark.unit
def test_main_show(temp_dir: Path, mock_conn_and_client: tuple[MagicMock | sqlite3.Connection, str]) -> None:
    """Test main() dispatches the ``show`` command for every database."""
    db_conn, client_class_name = mock_conn_and_client
    db = _resolve_database(client_class_name)
    _load_changelog(temp_dir, database=db)
    with (
        patch("migrateit.main.get_connection", return_value=db_conn),
        patch("migrateit.main.commands.cmd_show", return_value=0),
        patch("sys.argv", ["migrateit", "show", "--list", "--validate-sql"]),
    ):
        result = main()

    assert result == 0


@pytest.mark.unit
def test_main_migrate(temp_dir: Path, mock_conn_and_client: tuple[MagicMock | sqlite3.Connection, str]) -> None:
    """Test main() dispatches the ``migrate`` command for every database."""
    db_conn, client_class_name = mock_conn_and_client
    db = _resolve_database(client_class_name)
    _load_changelog(temp_dir, database=db)
    with (
        patch("migrateit.main.get_connection", return_value=db_conn),
        patch("migrateit.main.commands.cmd_run", return_value=0),
        patch("sys.argv", ["migrateit", "migrate", "0001_add_posts", "--fake", "--update-hash"]),
    ):
        result = main()

    assert result == 0


@pytest.mark.unit
def test_main_rollback(
    temp_dir: Path,
    mock_conn_and_client: tuple[MagicMock | sqlite3.Connection, str],
) -> None:
    """Test main() dispatches the ``rollback`` command for every database."""
    db_conn, client_class_name = mock_conn_and_client
    db = _resolve_database(client_class_name)
    _load_changelog(temp_dir, database=db)
    with (
        patch("migrateit.main.get_connection", return_value=db_conn),
        patch("migrateit.main.commands.cmd_run", return_value=0),
        patch("sys.argv", ["migrateit", "rollback", "0001_add_posts", "--fake"]),
    ):
        result = main()

    assert result == 0


@pytest.mark.unit
def test_main_squash(
    temp_dir: Path,
    mock_conn_and_client: tuple[MagicMock | sqlite3.Connection, str],
) -> None:
    """Test main() dispatches the ``squash`` command for every database."""
    db_conn, client_class_name = mock_conn_and_client
    db = _resolve_database(client_class_name)
    _load_changelog(temp_dir, database=db)
    with (
        patch("migrateit.main.get_connection", return_value=db_conn),
        patch("migrateit.main.commands.cmd_squash", return_value=0),
        patch("sys.argv", ["migrateit", "squash", "0001_a", "0003_c", "-n", "squashed"]),
    ):
        result = main()

    assert result == 0


@pytest.mark.unit
def test_main_no_command(temp_dir: Path) -> None:
    """Test main() prints help and returns 1 when no subcommand is given."""
    with patch("sys.argv", ["migrateit"]):
        result = main()

    assert result == 1
