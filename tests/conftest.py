import shutil
import sqlite3
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from migrateit.models.changelog import SupportedDatabase
from migrateit.tree import ROLLBACK_SPLIT_TAG

INIT_MIGRATION = "0000_migrateit.sql"
TEST_MIGRATIONS_TABLE = "migrations"


@pytest.fixture(autouse=True)
def suppress_write_line_b() -> Generator[None]:
    with patch("migrateit.reporters.output.write_line_b", lambda *_: None):
        yield


@pytest.fixture(autouse=True)
def suppress_write_line() -> Generator[None]:
    with patch("migrateit.reporters.output.write_line", lambda *_: None):
        yield


@pytest.fixture()
def temp_dir() -> Generator[Path]:
    """Create a temporary directory, cleaned up after the test."""
    d = Path(tempfile.mkdtemp())
    yield d
    shutil.rmtree(d)


def _make_psql_conn() -> MagicMock:
    """Create a MagicMock that satisfies isinstance(x, psycopg.Connection)."""
    conn: MagicMock = MagicMock()
    conn.__class__ = psycopg.Connection  # type: ignore[assignment]
    return conn


@pytest.fixture(params=list(SupportedDatabase), ids=lambda db: db.value)
def mock_conn_and_client(
    request: pytest.FixtureRequest,
) -> Generator[tuple[MagicMock | sqlite3.Connection, str]]:
    """Yield (connection, client_class_name) for every SupportedDatabase.

    The connection satisfies the match-statement ``isinstance`` check
    for its corresponding database type.
    """
    conn: Any

    if request.param is SupportedDatabase.POSTGRES:
        conn = _make_psql_conn()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        yield conn, "PsqlClient"
    elif request.param is SupportedDatabase.SQLITE:
        d = Path(tempfile.mkdtemp())
        db_path = d / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.autocommit = False
        yield conn, "SqliteClient"
        conn.close()
        import shutil

        shutil.rmtree(d)
    else:
        raise ValueError(f"Unsupported database type: {request.param}")


def create_migration_file(
    migrations_dir: Path,
    filename: str,
    sql: str = "SELECT 1;",
    rollback_sql: str = "SELECT 2;",
) -> Path:
    """Create a migration file with the standard rollback split tag."""
    migrations_dir.mkdir(parents=True, exist_ok=True)
    path = migrations_dir / filename
    with open(path, "w") as f:
        f.write(sql or f"-- Migration {filename}\n")
        f.write(f"{ROLLBACK_SPLIT_TAG}")
        f.write(f"\n\n{rollback_sql}")
    return path
