import pytest

from migrateit.models.changelog import Changelog
from migrateit.models.config import SupportedDatabase
from migrateit.models.migration import Migration

# --- changelog.to_dict tests ---


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_changelog_file_to_dict(database: SupportedDatabase) -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=())
    changelog = Changelog(version=1, database=database, migrations=[m])
    d = changelog.to_dict()
    assert d["version"] == 1
    assert d["database"] == database.value
    assert len(d["migrations"]) == 1


# --- changelog.to_json tests ---


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_changelog_file_to_json(database: SupportedDatabase) -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=())
    changelog = Changelog(version=1, database=database, migrations=[m])
    json_str = changelog.to_json()
    assert "version" in json_str
    assert "0000_init.sql" in json_str


# --- changelog.migrations_tree tests ---


@pytest.mark.unit
def test_build_tree_simple_chain() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_test.sql", parents=("0000_init.sql",)),
        Migration(name="0002_next.sql", parents=("0001_test.sql",)),
    ]
    changelog = Changelog(version=1, migrations=migrations)
    tree = changelog.migrations_tree
    assert list(tree.keys()) == ["0000_init.sql", "0001_test.sql", "0002_next.sql"]
    assert tree["0000_init.sql"] == [migrations[1]]
    assert tree["0001_test.sql"] == [migrations[2]]
    assert tree["0002_next.sql"] == []


@pytest.mark.unit
def test_build_tree_duplicated_name_raises() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_test.sql", parents=("0000_init.sql",)),
        Migration(name="0001_test.sql", parents=("0000_init.sql",)),
    ]
    changelog = Changelog(version=1, migrations=migrations)
    with pytest.raises(ValueError, match="duplicated"):
        changelog.migrations_tree


@pytest.mark.unit
def test_build_tree_with_multiple_parents() -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=())
    m2 = Migration(name="0001_branch_a.sql", parents=("0000_init.sql",))
    m3 = Migration(name="0002_branch_b.sql", parents=("0000_init.sql",))
    m4 = Migration(name="0003_merge.sql", parents=("0001_branch_a.sql", "0002_branch_b.sql"))
    changelog = Changelog(version=1, migrations=[m1, m2, m3, m4])
    tree = changelog.migrations_tree
    assert tree["0003_merge.sql"] == []
    assert m4 in tree["0001_branch_a.sql"]
    assert m4 in tree["0002_branch_b.sql"]
