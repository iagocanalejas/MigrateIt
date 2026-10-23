from pathlib import Path
from unittest.mock import patch

import pytest
from tests.unit.conftest import MIGRATION_NAME

from migrateit.constants import ROLLBACK_SPLIT_TAG
from migrateit.models.migration import (
    Migration,
    create_migration_directory,
    retrieve_migration_sqls,
    write_into_migration_file,
)
from migrateit.models.sql import remove_sql_comments

# --- get_content tests ---


@pytest.mark.unit
def test_get_migration_content_and_hash_no_rollback(temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    m = Migration(name=MIGRATION_NAME, initial=True, parents=())

    migrations_dir.mkdir(parents=True, exist_ok=True)
    with open(migrations_dir / MIGRATION_NAME, "w") as f:
        f.write("SELECT 1;")

    with pytest.raises(ValueError, match="No rollback"):
        m.content


@pytest.mark.unit
def test_get_migration_content_and_hash_more_than_one_rollback(temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    m = Migration(name=MIGRATION_NAME, initial=True, parents=())

    migrations_dir.mkdir(parents=True, exist_ok=True)
    with open(migrations_dir / MIGRATION_NAME, "w") as f:
        f.write("SELECT 1;" + ROLLBACK_SPLIT_TAG + "SELECT 2;" + ROLLBACK_SPLIT_TAG + "SELECT 3;")

    with pytest.raises(ValueError, match="Too many rollback"):
        m.content


@pytest.mark.unit
def test_get_migration_content_clean_comments(temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    m = Migration(name=MIGRATION_NAME, initial=True, parents=())

    migrations_dir.mkdir(parents=True, exist_ok=True)
    with open(migrations_dir / MIGRATION_NAME, "w") as f:
        f.write("-- Comment\n" + "SELECT 1;" + ROLLBACK_SPLIT_TAG + "SELECT 2;")

    assert "--" not in m.content[0]


# --- migration.to_dict tests ---


@pytest.mark.unit
def test_migration_to_dict_initial() -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=())
    d = m.to_dict()
    assert d == {"name": "0000_init.sql", "initial": True, "parents": ()}


@pytest.mark.unit
def test_migration_to_dict_with_parents() -> None:
    m = Migration(name="0001_add.sql", initial=False, parents=("0000_init.sql",))
    d = m.to_dict()
    assert d == {"name": "0001_add.sql", "initial": False, "parents": ("0000_init.sql",)}


# --- create_migration_directory tests ---


@pytest.mark.unit
def test_create_migration_directory(temp_dir: Path) -> None:
    d = temp_dir / "migrations"
    create_migration_directory()
    assert d.is_dir()


@pytest.mark.unit
def test_create_migration_directory_already_exists(temp_dir: Path) -> None:
    d = temp_dir / "migrations"
    d.mkdir(exist_ok=True)
    create_migration_directory()
    assert d.is_dir()


# --- retrieve_migration_sqls tests ---


@pytest.mark.unit
def test_retrieve_sql_with_both_sections(temp_dir: Path) -> None:
    file_path = temp_dir / "test.sql"
    content = (
        "-- Migration 0001_test.sql\n"
        "-- Created on 2024-01-01\n\n"
        "CREATE TABLE test (id INT);\n\n"
        "-- Rollback migration\n\n"
        "DROP TABLE test;\n"
    )
    file_path.write_text(content)
    sql, rollback = retrieve_migration_sqls(file_path)
    assert sql == "CREATE TABLE test (id INT);"
    assert rollback == "DROP TABLE test;"


@pytest.mark.unit
def test_retrieve_sql_only_forward(temp_dir: Path) -> None:
    file_path = temp_dir / "test.sql"
    content = "-- Migration 0001_test.sql\nSELECT 1;\n"
    file_path.write_text(content)
    sql, rollback = retrieve_migration_sqls(file_path)
    assert sql == "SELECT 1;"
    assert rollback is None


@pytest.mark.unit
def test_retrieve_sql_empty_forward(temp_dir: Path) -> None:
    file_path = temp_dir / "test.sql"
    content = f"\n\n{ROLLBACK_SPLIT_TAG}\n\nDROP TABLE test;\n"
    file_path.write_text(content)
    sql, rollback = retrieve_migration_sqls(file_path)
    assert sql == ""
    assert rollback == "DROP TABLE test;"


@pytest.mark.unit
def test_retrieve_sql_multiple_statements(temp_dir: Path) -> None:
    file_path = temp_dir / "test.sql"
    content = (
        "-- Migration\n"
        "CREATE TABLE a (id INT);\n"
        "CREATE TABLE b (id INT);\n\n"
        "-- Rollback migration\n\n"
        "DROP TABLE b; DROP TABLE a;"
    )
    file_path.write_text(content)
    sql, rollback = retrieve_migration_sqls(file_path)
    assert sql == "CREATE TABLE a (id INT);\nCREATE TABLE b (id INT);"
    assert rollback == "DROP TABLE b; DROP TABLE a;"


@pytest.mark.unit
def test_retrieve_sql_nonexistent_file(temp_dir: Path) -> None:
    bad_path = temp_dir / "nonexistent.sql"
    with pytest.raises(ValueError, match="is not a valid SQL file"):
        retrieve_migration_sqls(bad_path)


@pytest.mark.unit
def test_retrieve_sql_non_sql_file(temp_dir: Path) -> None:
    bad_path = temp_dir / "test.txt"
    bad_path.write_text("SELECT 1;")
    with pytest.raises(ValueError, match="is not a valid SQL file"):
        retrieve_migration_sqls(bad_path)


@pytest.mark.unit
def test_retrieve_sql_no_rollback_tag_removes_comments(temp_dir: Path) -> None:
    file_path = temp_dir / "test.sql"
    content = "-- Migration 0001_test.sql\n-- Created on 2024-01-01\n\nSELECT 1;\n"
    file_path.write_text(content)
    sql, rollback = retrieve_migration_sqls(file_path)
    assert sql is not None
    assert rollback is None
    assert "-- Migration" not in sql
    assert "-- Created on" not in sql
    assert sql == "SELECT 1;"


# --- write_into_migration_file tests ---


@pytest.mark.unit
def _make_migration_file(temp_dir: Path, filename: str = "0001_test.sql") -> Path:
    (temp_dir / "migrations").mkdir(parents=True, exist_ok=True)
    file_path = temp_dir / "migrations" / filename
    template = f"-- Migration {file_path.name}\n-- Created on 2024-01-01\n\n{ROLLBACK_SPLIT_TAG}"
    file_path.write_text(template)
    return file_path


@pytest.mark.unit
def test_write_sql_and_rollback(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    write_into_migration_file(file_path, sql="CREATE TABLE test (id INT);", rollback="DROP TABLE test;")
    content = file_path.read_text()
    assert "CREATE TABLE test (id INT);" in content
    assert "DROP TABLE test;" in content


@pytest.mark.unit
def test_write_sql_only(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    write_into_migration_file(file_path, sql="SELECT 1;", rollback=None)
    content = file_path.read_text()
    assert "SELECT 1;" in content


@pytest.mark.unit
def test_write_rollback_only(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    write_into_migration_file(file_path, sql=None, rollback="SELECT 2;")
    content = file_path.read_text()
    assert "SELECT 2;" in content


@pytest.mark.unit
def test_write_both_none_does_nothing(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    write_into_migration_file(file_path, sql=None, rollback=None)
    content = file_path.read_text()

    migration, rollback = content.split(ROLLBACK_SPLIT_TAG)
    assert remove_sql_comments(migration.strip()) == ""
    assert rollback.strip() == ""


@pytest.mark.unit
def test_write_no_rollback_tag_raises(temp_dir: Path) -> None:
    bad_path = temp_dir / "bad.sql"
    bad_path.write_text("SELECT 1;")
    with pytest.raises(ValueError, match="does not contain a rollback section"):
        write_into_migration_file(bad_path, sql="SELECT 1;", rollback=None)


@pytest.mark.unit
def test_write_sql_strips_newlines(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    write_into_migration_file(
        file_path,
        sql="\n\n  CREATE TABLE test (id INT);  \n\n",
        rollback="\n\n  DROP TABLE test;  \n\n",
    )
    content = file_path.read_text()
    assert "CREATE TABLE test (id INT);" in content
    assert "DROP TABLE test;" in content
    # Verify excessive blank lines are collapsed
    assert "\n\n\n\n" not in content


@pytest.mark.unit
def test_write_file_removed_while_processing(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    original_content = file_path.read_text()
    # Mock read_text to return valid content, then mock exists to return False
    # Only one exists() call happens (at line 110), so we need [False]
    with (
        patch.object(Path, "read_text", return_value=original_content),
        patch.object(Path, "exists", return_value=False),
    ):
        with pytest.raises(FileNotFoundError, match="does not exist. Removed while processing."):
            write_into_migration_file(file_path, sql="SELECT 1;", rollback=None)


# --- rename_squashed tests ---


@pytest.mark.unit
def test_rename_squashed(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir, filename="0001_test.sql")
    _make_migration_file(temp_dir, filename="squashed_0001_test.sql")
    m = Migration(name=file_path.name, initial=True, parents=())

    with pytest.raises(ValueError, match="already exists"):
        m.rename_squashed()
