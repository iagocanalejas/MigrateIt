import os
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest

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


def create_migration_file(
    migrations_dir: Path,
    filename: str,
    sql: str | None = None,
    rollback_sql: str | None = None,
) -> str:
    """Create a migration file with the standard rollback split tag."""
    migrations_dir.mkdir(parents=True, exist_ok=True)
    path = os.path.join(migrations_dir, filename)
    with open(path, "w") as f:
        f.write(sql or f"-- Migration {filename}\n")
        f.write(f"{ROLLBACK_SPLIT_TAG}")
        if rollback_sql:
            f.write(f"\n\n{rollback_sql}")
    return path
