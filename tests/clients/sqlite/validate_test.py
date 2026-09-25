import sqlite3
from pathlib import Path

import pytest

from migrateit.clients.sqlite import SqliteClient
from migrateit.models import Migration
from tests.conftest import create_migration_file


@pytest.mark.sqlite
def test_validate_valid_sql(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test SQL syntax validation with valid SQL."""
    create_migration_file(
        temp_dir / "migrations",
        "0001_valid.sql",
        sql="CREATE TABLE t (id INTEGER PRIMARY KEY);",
        rollback_sql="DROP TABLE t;",
    )

    sqlite_client.changelog.migrations.append(
        Migration(name="0001_valid.sql", initial=False, parents=["0000_migrateit.sql"])
    )
    assert sqlite_client.validate_sql_syntax(sqlite_client.changelog.migrations[1]) is None


@pytest.mark.sqlite
def test_validate_invalid_sql(sqlite_client: SqliteClient, temp_dir: Path) -> None:
    """Test SQL syntax validation with invalid SQL."""
    create_migration_file(
        temp_dir / "migrations",
        "0002_invalid.sql",
        sql="CRAete TABLE t (id INTEGER);",
        rollback_sql="DROP TABLE t;",
    )

    sqlite_client.changelog.migrations.append(
        Migration(name="0002_invalid.sql", initial=False, parents=["0000_migrateit.sql"])
    )
    result = sqlite_client.validate_sql_syntax(sqlite_client.changelog.migrations[1])
    assert result is not None
    assert isinstance(result[0], sqlite3.Error)
