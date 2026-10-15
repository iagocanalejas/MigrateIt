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
    cl = ChangelogRepository.create(path, database)
    assert path.exists()
    assert cl.version == 1
    assert cl.database == database
    assert cl.path == path


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_create_changelog_file_invalid_extension(temp_dir: Path, database: SupportedDatabase) -> None:
    bad_path = temp_dir / "migrations.txt"
    with pytest.raises(ValueError, match="must be a JSON file"):
        ChangelogRepository.create(bad_path, database)


@pytest.mark.unit
def test_create_changelog_file_already_exists(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    path.write_text("{}")
    with pytest.raises(FileExistsError, match="already exists"):
        ChangelogRepository.create(path, SupportedDatabase.POSTGRES)


# --- ChangelogRepository.save tests ---


@pytest.mark.unit
def test_save_changelog_file(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    path.touch()
    m = Migration(name="0000_init.sql", initial=True, parents=())
    cl = Changelog(version=2, database=SupportedDatabase.POSTGRES, migrations=[m], path=path)
    ChangelogRepository.save(cl)
    assert '"version": 2' in path.read_text()


@pytest.mark.unit
def test_save_changelog_file_not_exists(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    cl = Changelog(version=1, path=path)
    with pytest.raises(FileNotFoundError):
        ChangelogRepository.save(cl)


# --- ChangelogRepository.load tests ---


@pytest.mark.unit
def test_load_changelog_file_not_exists(temp_dir: Path) -> None:
    with pytest.raises(FileNotFoundError):
        ChangelogRepository.load(temp_dir / "changelog.json")


@pytest.mark.unit
def test_load_changelog_file_valid(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    cl = Changelog(version=1, migrations=[], path=path)
    path.write_text(cl.to_json())
    loaded = ChangelogRepository.load(path)
    assert loaded.version == 1


@pytest.mark.unit
def test_load_changelog_file_multiple_initial_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [
        Migration(name="0000_a.sql", initial=True, parents=()),
        Migration(name="0001_b.sql", initial=True, parents=()),
    ]
    cl = Changelog(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="exactly one initial migration"):
        ChangelogRepository.load(path)


@pytest.mark.unit
def test_load_changelog_file_initial_with_parents_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [Migration(name="0000_a.sql", initial=True, parents=("0001_b.sql",))]
    cl = Changelog(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="cannot have parents"):
        ChangelogRepository.load(path)


@pytest.mark.unit
def test_load_changelog_file_non_initial_without_parents_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [Migration(name="0000_a.sql", initial=True, parents=())]
    cl = Changelog(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    # Single initial migration is valid; adding a non-initial without parents
    migrations.append(Migration(name="0001_b.sql", parents=()))
    cl.migrations = migrations
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="must have parents"):
        ChangelogRepository.load(path)


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
        ChangelogRepository.load(path)


@pytest.mark.unit
def test_get_migration_by_name_prefix_collision_returns_first(temp_dir: Path) -> None:
    m1 = Migration(name="0001_init.sql", initial=True, parents=())
    m2 = Migration(name="0001_test.sql", parents=("0000_init.sql",))

    changelog_path = temp_dir / "changelog.json"
    changelog_path.touch()

    changelog = Changelog(version=1, migrations=[m1, m2], path=changelog_path)
    changelog_path.write_text(changelog.to_json())

    with pytest.raises(ValueError, match="duplicated index"):
        ChangelogRepository.load(changelog_path)


@pytest.mark.unit
def test_load_changelog_bad_migration_name(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="bad_name.sql", parents=("0000_init.sql",)),
    ]
    cl = Changelog(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="does not match the pattern"):
        ChangelogRepository.load(path)
