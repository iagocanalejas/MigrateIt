import shutil
import subprocess
from pathlib import Path

import pytest

from migrateit.models.config import SupportedDatabase
from migrateit.models.repository import ChangelogRepository

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _get_fixtures_path(database: SupportedDatabase) -> Path:
    value = database.value if database != SupportedDatabase.MYSQL else SupportedDatabase.MARIADB.value
    return FIXTURES_DIR / value


def _prepare_migrations_dir(database: SupportedDatabase, temp_dir: Path) -> None:
    shutil.copytree(_get_fixtures_path(database), temp_dir.absolute(), dirs_exist_ok=True)
    assert (temp_dir / "migrations" / "0000_migrateit.sql").exists()
    assert (temp_dir / "changelog.json").exists()

    result = subprocess.run(["migrateit", "migrate", "0000"], cwd=temp_dir, text=True, capture_output=True)
    if result.returncode != 0:
        raise AssertionError(
            f"Command 'migrateit migrate 0000' failed with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )


# NOTE: Migration fixtures will create:
# - 0001: Basic table with all the supported data types and constraints.
# - 0002: Custom specific database types added to the table created before.
# - 0003: Indexes created on the table.
# - 0004: Sequences created on the table applied to a new column.
# - 0005: Views created on the table.
# - 0006: Functions and triggers created on the table.


@pytest.mark.integration
def test_init(database: SupportedDatabase, temp_dir: Path) -> None:
    result = subprocess.run(
        ["migrateit", "init", database.value],
        cwd=temp_dir,
        capture_output=True,
        text=True,
        check=True,
    )

    migrations_dir = temp_dir / "migrations"
    changelog_file = temp_dir / "changelog.json"

    assert result.returncode == 0
    assert migrations_dir.exists()
    assert changelog_file.exists()
    assert (migrations_dir / "0000_migrateit.sql").exists()

    changelog = ChangelogRepository.load()
    assert changelog is not None
    assert len(changelog.migrations) == 1
    assert changelog.migrations[0].name == "0000_migrateit.sql"
    assert changelog.migrations[0].initial
    assert changelog.migrations[0].parents == []
    assert changelog.database == database


@pytest.mark.integration
def test_new(database: SupportedDatabase, temp_dir: Path) -> None:
    _prepare_migrations_dir(database, temp_dir)

    old_changelog = ChangelogRepository.load()
    subprocess.run(
        ["migrateit", "new", "test_migration", "--no-edit"],
        cwd=temp_dir,
        stdout=subprocess.DEVNULL,
    )
    new_changelog = ChangelogRepository.load()
    new_migratrion = new_changelog.migrations[-1]

    assert len(new_changelog.migrations) == len(old_changelog.migrations) + 1
    assert not new_migratrion.initial
    assert new_migratrion.parents == [old_changelog.migrations[-1].name]
    assert (temp_dir / "migrations" / new_migratrion.name).exists()


@pytest.mark.integration
def test_add(database: SupportedDatabase, temp_dir: Path) -> None:
    _prepare_migrations_dir(database, temp_dir)

    old_changelog = ChangelogRepository.load()
    subprocess.run(
        ["migrateit", "add", "0100", "-d", "0000"],
        cwd=temp_dir,
        stdout=subprocess.DEVNULL,
    )
    new_changelog = ChangelogRepository.load()
    new_migratrion = new_changelog.migrations[-1]

    assert len(new_changelog.migrations) == len(old_changelog.migrations) + 1
    assert not new_migratrion.initial
    assert new_migratrion.parents == [new_changelog.migrations[0].name]
    assert (temp_dir / "migrations" / new_migratrion.name).exists()


@pytest.mark.integration
def test_full_migrate_export_and_rollback(database: SupportedDatabase, temp_dir: Path) -> None:
    _prepare_migrations_dir(database, temp_dir)
    changelog = ChangelogRepository.load()

    migrate_result = subprocess.run(
        ["migrateit", "migrate"],
        cwd=temp_dir,
        capture_output=True,
        text=True,
        check=True,
    )

    assert migrate_result.returncode == 0
    assert "Migration complete" in migrate_result.stdout
    assert f"{len(changelog.migrations) - 1} migration(s) applied" in migrate_result.stdout

    export_result = subprocess.run(
        ["migrateit", "export"],
        cwd=temp_dir,
        capture_output=True,
        text=True,
        check=True,
    )

    assert export_result.returncode == 0
    assert "Full database exported" in export_result.stdout
    # NOTE: we reuse MARIADB for MYSQL
    db = database.value if database != SupportedDatabase.MYSQL else SupportedDatabase.MARIADB.value
    name = f"{len(changelog.migrations):04d}_{db}"
    assert name in export_result.stdout
    assert (temp_dir / "migrations" / f"{name}_backup.sql").exists()

    rollback_result = subprocess.run(
        ["migrateit", "rollback", "--zero"],
        cwd=temp_dir,
        capture_output=True,
        text=True,
        check=True,
    )

    assert rollback_result.returncode == 0
    assert "Rollback complete" in rollback_result.stdout
    assert f"{len(changelog.migrations)} migration(s) rolled back" in rollback_result.stdout


@pytest.mark.integration
def test_targeted_migration_and_rollback(database: SupportedDatabase, temp_dir: Path) -> None:
    _prepare_migrations_dir(database, temp_dir)

    migrate_result = subprocess.run(
        ["migrateit", "migrate", "0004"],
        cwd=temp_dir,
        capture_output=True,
        text=True,
        check=True,
    )

    assert migrate_result.returncode == 0
    assert "Migration complete" in migrate_result.stdout
    assert "4 migration(s) applied" in migrate_result.stdout

    rollback_result = subprocess.run(
        ["migrateit", "rollback", "0002"],
        cwd=temp_dir,
        capture_output=True,
        text=True,
        check=True,
    )
    print(rollback_result.stdout)
    print(rollback_result.stderr)

    assert rollback_result.returncode == 0
    assert "Rollback complete" in rollback_result.stdout
    assert "3 migration(s) rolled back" in rollback_result.stdout


@pytest.mark.integration
def test_show_and_validate(database: SupportedDatabase, temp_dir: Path) -> None:
    _prepare_migrations_dir(database, temp_dir)
    changelog = ChangelogRepository.load()

    result = subprocess.run(
        ["migrateit", "show", "--validate"],
        cwd=temp_dir,
        capture_output=True,
        text=True,
        check=True,
    )

    pre, summary = result.stdout.split("Summary")

    assert result.returncode == 0
    assert len([line for line in pre.splitlines() if "Not Applied" in line]) == len(changelog.migrations) - 1
    assert f"{len(changelog.migrations) - 1} migration(s) pending" in summary
    assert "SQL validation passed" in summary


@pytest.mark.integration
def test_drop(database: SupportedDatabase, temp_dir: Path) -> None:
    _prepare_migrations_dir(database, temp_dir)
    changelog = ChangelogRepository.load()
    migration_idx = f"{len(changelog.migrations) - 1:04d}"

    result = subprocess.run(
        ["migrateit", "drop", migration_idx],
        cwd=temp_dir,
        capture_output=True,
        text=True,
        check=True,
    )

    new_changelog = ChangelogRepository.load()
    migrations_dir = temp_dir / "migrations"
    assert result.returncode == 0
    assert "dropped and removed" in result.stdout
    assert len(new_changelog.migrations) == len(changelog.migrations) - 1
    assert len([m for m in migrations_dir.iterdir() if m.name.startswith(migration_idx)]) == 0


@pytest.mark.integration
def test_squash(database: SupportedDatabase, temp_dir: Path) -> None:
    _prepare_migrations_dir(database, temp_dir)

    old_changelog = ChangelogRepository.load()
    result = subprocess.run(
        ["migrateit", "squash", "0002", "0004"],
        cwd=temp_dir,
        capture_output=True,
        text=True,
        check=True,
    )
    new_changelog = ChangelogRepository.load()

    assert result.returncode == 0
    assert "Squashing migration: '0002" in result.stdout
    assert "Squashing migration: '0003" in result.stdout
    assert "Squashing migration: '0004" in result.stdout
    assert "removed 3 migration" in result.stdout
    assert f"added '{len(old_changelog.migrations):04d}_" in result.stdout
    assert len(new_changelog.migrations) == len(old_changelog.migrations) - 2
