from pathlib import Path

import pytest

from migrateit.models.changelog import ChangelogFile, Migration, SupportedDatabase
from migrateit.tree import (
    create_changelog_file,
    create_migration_directory,
    create_new_migration,
    load_changelog_file,
    save_changelog_file,
)


def test_create_migration_directory(temp_dir: Path) -> None:
    d = temp_dir / "migrations"
    create_migration_directory(d)
    assert d.is_dir()


def test_create_migration_directory_already_exists(temp_dir: Path) -> None:
    d = temp_dir / "migrations"
    d.mkdir()
    create_migration_directory(d)
    assert d.is_dir()


def test_create_changelog_file(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    cl = create_changelog_file(path, SupportedDatabase.POSTGRES)
    assert path.exists()
    assert cl.version == 1
    assert cl.database == SupportedDatabase.POSTGRES
    assert cl.path == path


def test_create_changelog_file_invalid_extension(temp_dir: Path) -> None:
    bad_path = temp_dir / "migrations.txt"
    with pytest.raises(ValueError):
        create_changelog_file(bad_path, SupportedDatabase.POSTGRES)


def test_load_changelog_file_not_exists(temp_dir: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_changelog_file(temp_dir / "changelog.json")


def test_save_changelog_file(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    path.touch()
    cl = ChangelogFile(version=2, path=path)
    save_changelog_file(cl)
    assert '"version": 2' in path.read_text()


def test_save_changelog_file_not_exists(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    cl = ChangelogFile(version=1, path=path)
    with pytest.raises(FileNotFoundError):
        save_changelog_file(cl)


def test_create_new_migration_success(temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir()
    path = temp_dir / "changelog.json"
    cl = create_changelog_file(path, SupportedDatabase.POSTGRES)

    create_new_migration(cl, migrations_dir, "init")
    created_files = sorted(migrations_dir.iterdir())
    assert len(created_files) == 1
    assert created_files[0].name == "0000_init.sql"

    migrations = load_changelog_file(path)
    assert len(migrations.migrations) == 1
    assert migrations.migrations[0].name.endswith("init.sql")


def test_create_new_migration_with_dependencies(temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir()
    path = temp_dir / "changelog.json"
    cl = create_changelog_file(path, SupportedDatabase.POSTGRES)

    create_new_migration(cl, migrations_dir, "init")
    create_new_migration(cl, migrations_dir, "add_users", dependencies=["0000"])
    created_files = sorted(migrations_dir.iterdir())
    assert len(created_files) == 2
    assert created_files[0].name == "0000_init.sql"
    assert created_files[1].name == "0001_add_users.sql"

    migrations = load_changelog_file(path)
    assert len(migrations.migrations) == 2
    assert "init" in migrations.migrations[1].parents[0]


def test_create_new_migration_invalid_name(temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir()
    path = temp_dir / "changelog.json"
    cl = create_changelog_file(path, SupportedDatabase.POSTGRES)

    with pytest.raises(ValueError):
        create_new_migration(cl, migrations_dir, "123-bad-name")

    with pytest.raises(ValueError):
        create_new_migration(cl, migrations_dir, "")


def test_load_changelog_file_valid(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    cl = ChangelogFile(version=1, migrations=[], path=path)
    path.write_text(cl.to_json())
    loaded = load_changelog_file(path)
    assert loaded.version == 1


def test_load_changelog_file_multiple_initial_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [
        Migration(name="0000_a.sql", initial=True, parents=[]),
        Migration(name="0001_b.sql", initial=True, parents=[]),
    ]
    cl = ChangelogFile(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError):
        load_changelog_file(path)


def test_load_changelog_file_initial_with_parents_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [Migration(name="0000_a.sql", initial=True, parents=["0001_b.sql"])]
    cl = ChangelogFile(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError):
        load_changelog_file(path)


def test_load_changelog_file_non_initial_without_parents_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [Migration(name="0000_a.sql", initial=True, parents=[])]
    cl = ChangelogFile(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    migrations.append(Migration(name="0001_b.sql", parents=[]))
    cl.migrations = migrations
    path.write_text(cl.to_json())
    with pytest.raises(ValueError):
        load_changelog_file(path)
