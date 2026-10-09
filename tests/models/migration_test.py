import json
from pathlib import Path

import pytest
from tests.clients._clients_test import MIGRATION_NAME

from migrateit.constants import ROLLBACK_SPLIT_TAG
from migrateit.models.changelog import ChangelogFile, SupportedDatabase
from migrateit.models.migration import (
    Migration,
    create_migration_directory,
    retrieve_migration_sqls,
    write_into_migration_file,
)

# --- migration.is_valid_name tests ---


@pytest.mark.unit
def test_valid_sql_file(temp_dir: Path) -> None:
    path = temp_dir / "0001_test.sql"
    path.write_bytes(b"SELECT 1;")
    assert Migration.is_valid_name(path) is True


@pytest.mark.unit
def test_valid_file_no_sql_extension(temp_dir: Path) -> None:
    path = temp_dir / "0001_test.py"
    path.write_bytes(b"SELECT 1;")
    assert Migration.is_valid_name(path) is False


@pytest.mark.unit
def test_valid_file_no_prefix(temp_dir: Path) -> None:
    path = temp_dir / "test.sql"
    path.write_bytes(b"SELECT 1;")
    assert Migration.is_valid_name(path) is False


@pytest.mark.unit
def test_valid_nonexistent_file() -> None:
    path = Path("/nonexistent/file/0001_test.sql")
    assert Migration.is_valid_name(path) is False


# --- migration.is_same_migration_name tests ---


@pytest.mark.unit
@pytest.mark.parametrize(
    "name1,name2, expected",
    [
        ("0001_test.sql", "0001_test.sql", True),
        ("0001_test.sql", "0001_test_v2.sql", True),
        ("0001_test.sql", "0002_test.sql", False),
        ("test.sql", "test.sql", True),
        ("", "0001_test.sql", False),
        ("0001_test.sql", "", False),
        ("", "", False),
    ],
)
def test_migration_is_same_migration_name(name1: str, name2: str, expected: bool) -> None:
    assert Migration.is_same_migration_name(name1, name2) is expected


# --- get_content_and_hash tests ---


@pytest.mark.unit
def test_get_migration_content_and_hash_no_rollback(temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    m = Migration(name=MIGRATION_NAME, initial=True, parents=())

    migrations_dir.mkdir(parents=True, exist_ok=True)
    with open(migrations_dir / MIGRATION_NAME, "w") as f:
        f.write("SELECT 1;")

    with pytest.raises(ValueError, match="No rollback"):
        m.get_content_and_hash(migrations_dir)


@pytest.mark.unit
def test_get_migration_content_and_hash_more_than_one_rollback(temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    m = Migration(name=MIGRATION_NAME, initial=True, parents=())

    migrations_dir.mkdir(parents=True, exist_ok=True)
    with open(migrations_dir / MIGRATION_NAME, "w") as f:
        f.write("SELECT 1;" + ROLLBACK_SPLIT_TAG + "SELECT 2;" + ROLLBACK_SPLIT_TAG + "SELECT 3;")

    with pytest.raises(ValueError, match="Too many rollback"):
        m.get_content_and_hash(migrations_dir)


@pytest.mark.unit
def test_get_migration_content_clean_comments(temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    m = Migration(name=MIGRATION_NAME, initial=True, parents=())

    migrations_dir.mkdir(parents=True, exist_ok=True)
    with open(migrations_dir / MIGRATION_NAME, "w") as f:
        f.write("-- Comment\n" + "SELECT 1;" + ROLLBACK_SPLIT_TAG + "SELECT 2;")

    code, _, _ = m.get_content_and_hash(migrations_dir)
    assert "--" not in code


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


# --- migration.from_json tests ---


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_changelog_file_from_json_valid(database: SupportedDatabase) -> None:
    json_str = json.dumps(
        {
            "version": 1,
            "database": database.value,
            "migrations": [{"name": "0000_init.sql", "initial": True, "parents": []}],
        }
    )
    path = Path("/tmp/test_changelog.json")
    changelog = ChangelogFile.from_json(json_str, path)
    assert changelog.version == 1
    assert changelog.database == database
    assert len(changelog.migrations) == 1
    assert changelog.migrations[0].name == "0000_init.sql"
    assert changelog.migrations[0].initial is True


@pytest.mark.unit
def test_changelog_file_from_json_no_migrations() -> None:
    json_str = '{"version": 1}'
    path = Path("/tmp/test_changelog.json")
    changelog = ChangelogFile.from_json(json_str, path)
    assert changelog.version == 1
    assert changelog.migrations == []


@pytest.mark.unit
def test_changelog_file_from_json_invalid_json() -> None:
    with pytest.raises(ValueError, match="Expecting property name"):
        ChangelogFile.from_json("{invalid json", Path("/tmp/test.json"))


@pytest.mark.unit
def test_changelog_file_from_json_missing_version() -> None:
    with pytest.raises(ValueError, match="'version'"):
        ChangelogFile.from_json('{"migrations": []}', Path("/tmp/test.json"))


# --- create_migration_directory tests ---


@pytest.mark.unit
def test_create_migration_directory(temp_dir: Path) -> None:
    d = temp_dir / "migrations"
    create_migration_directory(d)
    assert d.is_dir()


@pytest.mark.unit
def test_create_migration_directory_already_exists(temp_dir: Path) -> None:
    d = temp_dir / "migrations"
    d.mkdir(exist_ok=True)
    create_migration_directory(d)
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
    file_path = temp_dir / filename
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
def test_write_both_none_raises(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    with pytest.raises(ValueError, match="At least one of sql or rollback must be provided"):
        write_into_migration_file(file_path, sql=None, rollback=None)


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
