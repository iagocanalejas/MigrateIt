from pathlib import Path

import pytest

from migrateit.models.changelog import Changelog
from migrateit.models.config import SupportedDatabase
from migrateit.models.migration import Migration
from migrateit.models.repository import ChangelogRepository

# --- ChangelogRepository.create tests ---


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_create_changelog_file(temp_dir: Path, database: SupportedDatabase) -> None:
    path = temp_dir / "changelog.json"
    cl = ChangelogRepository.create(database)
    assert path.exists()
    assert cl.version == 1
    assert cl.database == database
    assert cl.path == path


# --- ChangelogRepository.save tests ---


@pytest.mark.unit
def test_save_changelog_file(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    path.touch()
    m = Migration(name="0000_init.sql", initial=True, parents=())
    cl = Changelog(version=2, database=SupportedDatabase.POSTGRES, migrations=[m])
    ChangelogRepository.save(cl)
    assert '"version": 2' in path.read_text()


@pytest.mark.unit
def test_save_changelog_file_not_exists(temp_dir: Path) -> None:
    cl = Changelog(version=1)
    with pytest.raises(FileNotFoundError):
        ChangelogRepository.save(cl)


# --- ChangelogRepository.load tests ---


@pytest.mark.unit
def test_load_changelog_file_not_exists(temp_dir: Path) -> None:
    with pytest.raises(FileNotFoundError):
        ChangelogRepository.load()


@pytest.mark.unit
def test_load_changelog_file_valid(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    cl = Changelog(version=1, migrations=[])
    path.write_text(cl.to_json())
    loaded = ChangelogRepository.load()
    assert loaded.version == 1


@pytest.mark.unit
def test_load_changelog_file_multiple_initial_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [
        Migration(name="0000_a.sql", initial=True, parents=()),
        Migration(name="0001_b.sql", initial=True, parents=()),
    ]
    cl = Changelog(version=1, migrations=migrations)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="exactly one initial migration"):
        ChangelogRepository.load()


@pytest.mark.unit
def test_load_changelog_file_initial_with_parents_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [Migration(name="0000_a.sql", initial=True, parents=("0001_b.sql",))]
    cl = Changelog(version=1, migrations=migrations)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="cannot have parents"):
        ChangelogRepository.load()


@pytest.mark.unit
def test_load_changelog_file_non_initial_without_parents_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [Migration(name="0000_a.sql", initial=True, parents=())]
    cl = Changelog(version=1, migrations=migrations)
    path.write_text(cl.to_json())
    # Single initial migration is valid; adding a non-initial without parents
    migrations.append(Migration(name="0001_b.sql", parents=()))
    cl.migrations = migrations
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="must have parents"):
        ChangelogRepository.load()


@pytest.mark.unit
def test_load_changelog_with_non_existing_parent(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_test.sql", parents=("9999_missing.sql",)),
    ]
    changelog = Changelog(version=1, migrations=migrations)
    path.write_text(changelog.to_json())

    with pytest.raises(ValueError, match="non-existent parent"):
        ChangelogRepository.load()


@pytest.mark.unit
def test_get_migration_by_name_prefix_collision_returns_first(temp_dir: Path) -> None:
    m1 = Migration(name="0001_init.sql", initial=True, parents=())
    m2 = Migration(name="0001_test.sql", parents=("0000_init.sql",))

    changelog_path = temp_dir / "changelog.json"
    changelog_path.touch()

    changelog = Changelog(version=1, migrations=[m1, m2])
    changelog_path.write_text(changelog.to_json())

    with pytest.raises(ValueError, match="duplicated index"):
        ChangelogRepository.load()


@pytest.mark.unit
def test_load_changelog_bad_migration_name(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="bad_name.sql", parents=("0000_init.sql",)),
    ]
    cl = Changelog(version=1, migrations=migrations)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="does not match the pattern"):
        ChangelogRepository.load()
