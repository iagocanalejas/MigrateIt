import json
from pathlib import Path

import pytest

from migrateit.models.changelog import ChangelogFile, SupportedDatabase
from migrateit.models.migration import Migration

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
def test_migration_is_same_migration_name_exact_match() -> None:
    assert Migration.is_same_migration_name("0001_test.sql", "0001_test.sql") is True


@pytest.mark.unit
def test_migration_is_same_migration_name_prefix_match() -> None:
    assert Migration.is_same_migration_name("0001_test_v2.sql", "0001_test.sql") is True


@pytest.mark.unit
def test_migration_is_same_migration_name_different_prefix() -> None:
    assert Migration.is_same_migration_name("0002_test.sql", "0001_test.sql") is False


@pytest.mark.unit
def test_migration_is_same_migration_name_no_prefix() -> None:
    assert Migration.is_same_migration_name("test.sql", "test.sql") is True


# --- migration.to_dict tests ---


@pytest.mark.unit
def test_migration_to_dict_initial() -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=[])
    d = m.to_dict()
    assert d == {"name": "0000_init.sql", "initial": True, "parents": []}


@pytest.mark.unit
def test_migration_to_dict_with_parents() -> None:
    m = Migration(name="0001_add.sql", initial=False, parents=["0000_init.sql"])
    d = m.to_dict()
    assert d == {"name": "0001_add.sql", "initial": False, "parents": ["0000_init.sql"]}


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


# --- migration.is_same_migration_name tests ---


@pytest.mark.unit
def test_migration_is_same_migration_name_empty_first() -> None:
    assert Migration.is_same_migration_name("", "0001_test.sql") is False


@pytest.mark.unit
def test_migration_is_same_migration_name_empty_second() -> None:
    assert Migration.is_same_migration_name("0001_test.sql", "") is False


@pytest.mark.unit
def test_migration_is_same_migration_name_both_empty() -> None:
    assert Migration.is_same_migration_name("", "") is False
