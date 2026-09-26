from pathlib import Path

import pytest

from migrateit.models.changelog import ChangelogFile, SupportedDatabase
from migrateit.models.migration import Migration


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


@pytest.mark.unit
def test_changelog_file_from_json_valid() -> None:
    json_str = (
        '{"version": 1, "database": "postgres", '
        '"migrations": [{"name": "0000_init.sql", "initial": true, "parents": []}]}'
    )
    path = Path("/tmp/test_changelog.json")
    changelog = ChangelogFile.from_json(json_str, path)
    assert changelog.version == 1
    assert changelog.database == SupportedDatabase.POSTGRES
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
    with pytest.raises(ValueError):
        ChangelogFile.from_json("{invalid json", Path("/tmp/test.json"))


@pytest.mark.unit
def test_changelog_file_from_json_missing_version() -> None:
    with pytest.raises(ValueError):
        ChangelogFile.from_json('{"migrations": []}', Path("/tmp/test.json"))


@pytest.mark.unit
def test_changelog_file_to_dict() -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=[])
    changelog = ChangelogFile(version=1, database=SupportedDatabase.POSTGRES, migrations=[m])
    d = changelog.to_dict()
    assert d["version"] == 1
    assert d["database"] == "postgres"
    assert len(d["migrations"]) == 1


@pytest.mark.unit
def test_changelog_file_to_json() -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=[])
    changelog = ChangelogFile(version=1, database=SupportedDatabase.POSTGRES, migrations=[m])
    json_str = changelog.to_json()
    assert "version" in json_str
    assert "0000_init.sql" in json_str


@pytest.mark.unit
def test_changelog_file_exist_migration_by_name_exact() -> None:
    m = Migration(name="0001_test.sql", parents=["0000_init.sql"])
    changelog = ChangelogFile(version=1, migrations=[m])
    assert changelog.exist_migration_by_name("0001_test.sql") is True


@pytest.mark.unit
def test_changelog_file_exist_migration_by_name_prefix() -> None:
    m = Migration(name="0001_test.sql", parents=["0000_init.sql"])
    changelog = ChangelogFile(version=1, migrations=[m])
    assert changelog.exist_migration_by_name("0001_other.sql") is True


@pytest.mark.unit
def test_changelog_file_exist_migration_by_name_not_found() -> None:
    m = Migration(name="0001_test.sql", parents=["0000_init.sql"])
    changelog = ChangelogFile(version=1, migrations=[m])
    assert changelog.exist_migration_by_name("0002_other.sql") is False


@pytest.mark.unit
def test_changelog_file_exist_migration_by_name_abs_path() -> None:
    m = Migration(name="0001_test.sql", parents=["0000_init.sql"])
    changelog = ChangelogFile(version=1, migrations=[m])
    assert changelog.exist_migration_by_name("/some/path/0001_test.sql") is True


@pytest.mark.unit
def test_changelog_file_get_migration_by_name() -> None:
    m1 = Migration(name="0001_test.sql", parents=["0000_init.sql"])
    m2 = Migration(name="0002_other.sql", parents=["0001_test.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2])
    result = changelog.get_migration_by_name("0001")
    assert result.name == "0001_test.sql"


@pytest.mark.unit
def test_changelog_file_get_migration_by_name_full() -> None:
    m = Migration(name="0001_test.sql", parents=["0000_init.sql"])
    changelog = ChangelogFile(version=1, migrations=[m])
    result = changelog.get_migration_by_name("0001_test.sql")
    assert result.name == "0001_test.sql"


@pytest.mark.unit
def test_changelog_file_get_migration_by_name_not_found() -> None:
    m = Migration(name="0001_test.sql", parents=["0000_init.sql"])
    changelog = ChangelogFile(version=1, migrations=[m])
    with pytest.raises(ValueError):
        changelog.get_migration_by_name("0002_nonexistent")


@pytest.mark.unit
def test_changelog_file_get_migration_by_name_abs_path() -> None:
    m = Migration(name="0001_test.sql", parents=["0000_init.sql"])
    changelog = ChangelogFile(version=1, migrations=[m])
    result = changelog.get_migration_by_name("/some/path/0001_test.sql")
    assert result.name == "0001_test.sql"


@pytest.mark.unit
def test_migration_is_same_migration_name_empty_first() -> None:
    assert Migration.is_same_migration_name("", "0001_test.sql") is False


@pytest.mark.unit
def test_migration_is_same_migration_name_empty_second() -> None:
    assert Migration.is_same_migration_name("0001_test.sql", "") is False


@pytest.mark.unit
def test_migration_is_same_migration_name_both_empty() -> None:
    assert Migration.is_same_migration_name("", "") is False


# --- changelog.migrations_tree tests ---


@pytest.mark.unit
def test_build_tree_simple_chain() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_test.sql", parents=["0000_init.sql"]),
        Migration(name="0002_next.sql", parents=["0001_test.sql"]),
    ]
    changelog = ChangelogFile(version=1, migrations=migrations)
    tree = changelog.migrations_tree
    assert list(tree.keys()) == ["0000_init.sql", "0001_test.sql", "0002_next.sql"]
    assert tree["0000_init.sql"] == [migrations[1]]
    assert tree["0001_test.sql"] == [migrations[2]]
    assert tree["0002_next.sql"] == []


@pytest.mark.unit
def test_build_tree_with_multiple_parents() -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_branch_a.sql", parents=["0000_init.sql"])
    m3 = Migration(name="0002_branch_b.sql", parents=["0000_init.sql"])
    m4 = Migration(name="0003_merge.sql", parents=["0001_branch_a.sql", "0002_branch_b.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2, m3, m4])
    tree = changelog.migrations_tree
    assert tree["0003_merge.sql"] == []
    assert m4 in tree["0001_branch_a.sql"]
    assert m4 in tree["0002_branch_b.sql"]
