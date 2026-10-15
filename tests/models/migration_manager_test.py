from pathlib import Path
from unittest.mock import patch

import pytest

from migrateit.models.changelog import Changelog
from migrateit.models.migration import Migration, MigrationStatus
from migrateit.models.migration_manager import MigrationManager

# --- create_new_migration — error paths ---


@pytest.mark.unit
def test_create_new_migration_invalid_name(temp_dir: Path) -> None:
    path = temp_dir / "migrations"
    path.mkdir(parents=True, exist_ok=True)
    changelog = Changelog(version=1, migrations=[Migration(name="0000_init.sql", initial=True, parents=())])
    manager = MigrationManager(changelog)
    with pytest.raises(ValueError, match="not a valid identifier"):
        manager.create_new_migration(migrations_dir=path, name="invalid-name")


@pytest.mark.unit
def test_create_new_migration_dependency_not_found(temp_dir: Path) -> None:
    path = temp_dir / "migrations"
    path.mkdir(parents=True, exist_ok=True)
    changelog = Changelog(version=1, migrations=[Migration(name="0000_init.sql", initial=True, parents=())])
    manager = MigrationManager(changelog)
    with pytest.raises(ValueError, match="Some dependencies"):
        manager.create_new_migration(migrations_dir=path, name="test", dependencies=("9999_missing",))


@pytest.mark.unit
def test_create_new_migration_initial_with_deps_raises(temp_dir: Path) -> None:
    path = temp_dir / "migrations"
    path.mkdir(parents=True, exist_ok=True)
    changelog = Changelog(version=1, migrations=[])
    manager = MigrationManager(changelog)
    fake_migration = Migration(name="0000_init.sql", initial=True)
    with (
        patch.object(manager, "exist_migration_by_name", return_value=True),
        patch.object(manager, "get_migration_by_name", return_value=fake_migration),
    ):
        with pytest.raises(ValueError, match="Initial migration cannot have dependencies"):
            manager.create_new_migration(migrations_dir=path, name="test", dependencies=("0000_init.sql",))


# --- get_migration_by_name — error path ---


@pytest.mark.unit
def test_get_migration_by_name_invalid_prefix() -> None:
    changelog = Changelog(version=1, migrations=[Migration(name="0000_init.sql", initial=True)])
    manager = MigrationManager(changelog)
    with pytest.raises(ValueError, match="Invalid"):
        manager.get_migration_by_name("not_a_number")


# --- manager.exist_migration_by_name tests ---


@pytest.mark.unit
def test_changelog_file_exist_migration_by_name_exact() -> None:
    m = Migration(name="0001_test.sql", parents=("0000_init.sql",))
    changelog = Changelog(version=1, migrations=[m])
    manager = MigrationManager(changelog)
    assert manager.exist_migration_by_name("0001_test.sql") is True


@pytest.mark.unit
def test_changelog_file_exist_migration_by_name_prefix() -> None:
    m = Migration(name="0001_test.sql", parents=("0000_init.sql",))
    changelog = Changelog(version=1, migrations=[m])
    manager = MigrationManager(changelog)
    assert manager.exist_migration_by_name("0001_other.sql") is True


@pytest.mark.unit
def test_changelog_file_exist_migration_by_name_not_found() -> None:
    m = Migration(name="0001_test.sql", parents=("0000_init.sql",))
    changelog = Changelog(version=1, migrations=[m])
    manager = MigrationManager(changelog)
    assert manager.exist_migration_by_name("0002_other.sql") is False


@pytest.mark.unit
def test_changelog_file_exist_migration_by_name_abs_path() -> None:
    m = Migration(name="0001_test.sql", parents=("0000_init.sql",))
    changelog = Changelog(version=1, migrations=[m])
    manager = MigrationManager(changelog)
    assert manager.exist_migration_by_name("/some/path/0001_test.sql") is True


# --- manager.get_migration_by_name tests ---


@pytest.mark.unit
def test_changelog_file_get_migration_by_name() -> None:
    m1 = Migration(name="0001_test.sql", parents=("0000_init.sql",))
    m2 = Migration(name="0002_other.sql", parents=("0001_test.sql",))
    changelog = Changelog(version=1, migrations=[m1, m2])
    manager = MigrationManager(changelog)
    result = manager.get_migration_by_name("0001")
    assert result.name == "0001_test.sql"


@pytest.mark.unit
def test_changelog_file_get_migration_by_name_full() -> None:
    m = Migration(name="0001_test.sql", parents=("0000_init.sql",))
    changelog = Changelog(version=1, migrations=[m])
    manager = MigrationManager(changelog)
    result = manager.get_migration_by_name("0001_test.sql")
    assert result.name == "0001_test.sql"


@pytest.mark.unit
def test_changelog_file_get_migration_by_name_not_found() -> None:
    m = Migration(name="0001_test.sql", parents=("0000_init.sql",))
    changelog = Changelog(version=1, migrations=[m])
    manager = MigrationManager(changelog)
    with pytest.raises(ValueError, match="not found"):
        manager.get_migration_by_name("0002_nonexistent")


@pytest.mark.unit
def test_changelog_file_get_migration_by_name_abs_path() -> None:
    m = Migration(name="0001_test.sql", parents=("0000_init.sql",))
    changelog = Changelog(version=1, migrations=[m])
    manager = MigrationManager(changelog)
    result = manager.get_migration_by_name("/some/path/0001_test.sql")
    assert result.name == "0001_test.sql"


# --- _get_next_migration_path — overwrite index ---


@pytest.mark.unit
def test_get_next_migration_path_overwrite_index(temp_dir: Path) -> None:
    path = temp_dir / "migrations"
    path.mkdir(parents=True, exist_ok=True)
    # Create a file with index "0002" to match the next migration index
    # (we have 2 existing migrations, so next is "0002")
    existing = path / "0002_other.sql"
    existing.write_text("header\n\n-- Rollback migration\n\nrollback\n")

    changelog = Changelog(
        version=1,
        migrations=[
            Migration(name="0000_init.sql", initial=True, parents=()),
            Migration(name="0001_existing.sql", parents=("0000_init.sql",)),
        ],
    )
    manager = MigrationManager(changelog)

    with patch("migrateit.models.migration_manager.inquirer.confirm", return_value=True):
        result = manager._get_next_migration_path(path, "new_name")

    assert result.name == "0002_new_name.sql"
    # The old file with same index should be gone
    assert not existing.exists()


@pytest.mark.unit
def test_get_next_migration_path_overwrite_index_declined(temp_dir: Path) -> None:
    path = temp_dir / "migrations"
    path.mkdir(parents=True, exist_ok=True)
    existing = path / "0002_other.sql"
    existing.write_text("header\n\n-- Rollback migration\n\nrollback\n")

    changelog = Changelog(
        version=1,
        migrations=[
            Migration(name="0000_init.sql", initial=True, parents=()),
            Migration(name="0001_existing.sql", parents=("0000_init.sql",)),
        ],
    )
    manager = MigrationManager(changelog)

    with patch("migrateit.models.migration_manager.inquirer.confirm", return_value=False):
        with pytest.raises(FileExistsError, match="Migration index 0002 already exists"):
            manager._get_next_migration_path(path, "new_name")


# --- manager.build_migration_plan tests ---


@pytest.mark.unit
def test_build_plan_simple_forward() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_add_table.sql", parents=("0000_init.sql",)),
    ]
    changelog = Changelog(version=1, migrations=migrations)
    manager = MigrationManager(changelog)
    statuses = {"0000_init.sql": MigrationStatus.APPLIED, "0001_add_table.sql": MigrationStatus.NOT_APPLIED}

    plan = manager.build_migration_plan(statuses)
    assert len(plan) == 1
    assert plan[0].name == "0001_add_table.sql"


@pytest.mark.unit
def test_build_plan_diamond_requeue_visited() -> None:
    """Diamond DAG with cross-parent dependency."""
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_left.sql", parents=("0000_init.sql",)),
        Migration(name="0002_right.sql", parents=("0000_init.sql", "0001_left.sql")),
        Migration(name="0003_merge.sql", parents=("0001_left.sql", "0002_right.sql")),
    ]
    changelog = Changelog(version=1, migrations=migrations)
    manager = MigrationManager(changelog)
    statuses = dict.fromkeys([m.name for m in migrations], MigrationStatus.NOT_APPLIED)

    plan = manager.build_migration_plan(statuses)
    assert [m.name for m in plan] == ["0000_init.sql", "0001_left.sql", "0002_right.sql", "0003_merge.sql"]


@pytest.mark.unit
def test_build_plan_unvisited_parents() -> None:
    """DAG where a node is enqueued before its parent is visited."""
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_init.sql", parents=("0000_init.sql",)),
        Migration(name="0002_init.sql", parents=("0001_init.sql",)),
        Migration(name="0003_init.sql", parents=("0000_init.sql", "0002_init.sql")),
    ]
    changelog = Changelog(version=1, migrations=migrations)
    manager = MigrationManager(changelog)
    statuses = dict.fromkeys([m.name for m in migrations], MigrationStatus.NOT_APPLIED)

    plan = manager.build_migration_plan(statuses)
    assert [m.name for m in plan] == ["0000_init.sql", "0001_init.sql", "0002_init.sql", "0003_init.sql"]


@pytest.mark.unit
def test_build_plan_already_visited_neighbor() -> None:
    """DAG where a neighbor is already in queue when we try to add it."""
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_init.sql", parents=("0000_init.sql",)),
        Migration(name="0002_init.sql", parents=("0001_init.sql", "0000_init.sql")),
    ]
    changelog = Changelog(version=1, migrations=migrations)
    manager = MigrationManager(changelog)
    statuses = dict.fromkeys([m.name for m in migrations], MigrationStatus.NOT_APPLIED)

    plan = manager.build_migration_plan(statuses_map=statuses, target_migration=migrations[2])
    assert [m.name for m in plan] == ["0001_init.sql", "0000_init.sql", "0002_init.sql"]


@pytest.mark.unit
def test_build_plan_rollback_applied() -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=())
    m2 = Migration(name="0001_add.sql", parents=("0000_init.sql",))
    m3 = Migration(name="0002_more.sql", parents=("0001_add.sql",))
    changelog = Changelog(version=1, migrations=[m1, m2, m3])
    manager = MigrationManager(changelog)
    statuses: dict[str, MigrationStatus] = {
        "0000_init.sql": MigrationStatus.APPLIED,
        "0001_add.sql": MigrationStatus.APPLIED,
        "0002_more.sql": MigrationStatus.NOT_APPLIED,
    }
    plan = manager.build_migration_plan(statuses, m2, is_rollback=True)
    assert len(plan) == 1
    assert plan[0].name == "0001_add.sql"


@pytest.mark.unit
def test_build_plan_bottom_up() -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=())
    m2 = Migration(name="0001_add.sql", parents=("0000_init.sql",))
    changelog = Changelog(version=1, migrations=[m1, m2])
    manager = MigrationManager(changelog)
    statuses: dict[str, MigrationStatus] = {
        "0000_init.sql": MigrationStatus.APPLIED,
        "0001_add.sql": MigrationStatus.NOT_APPLIED,
    }
    plan = manager.build_migration_plan(statuses, m2, is_rollback=False)
    assert len(plan) == 1
    assert plan[0].name == "0001_add.sql"


@pytest.mark.unit
def test_build_plan_rollback_no_target() -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=())
    changelog = Changelog(version=1, migrations=[m])
    manager = MigrationManager(changelog)
    statuses: dict[str, MigrationStatus] = {"0000_init.sql": MigrationStatus.APPLIED}
    with pytest.raises(ValueError, match="Target migration is required for rollback"):
        manager.build_migration_plan(statuses, is_rollback=True)


# --- manager.find_path tests ---


@pytest.mark.unit
def test_find_path_same_node() -> None:
    cl = Changelog(version=1, migrations=[Migration(name="0001_a")])
    manager = MigrationManager(cl)
    path = manager.find_path("0001_a", "0001_a")
    assert path == ["0001_a"]


@pytest.mark.unit
def test_find_path_linear() -> None:
    cl = Changelog(
        version=1,
        migrations=[
            Migration(name="0001_a"),
            Migration(name="0002_b", parents=("0001_a",)),
            Migration(name="0003_c", parents=("0002_b",)),
        ],
    )
    manager = MigrationManager(cl)
    path = manager.find_path("0001_a", "0003_c")
    assert path == ["0001_a", "0002_b", "0003_c"]


@pytest.mark.unit
def test_find_path_no_path() -> None:
    cl = Changelog(
        version=1,
        migrations=[
            Migration(name="0001_a"),
            Migration(name="0002_b"),
        ],
    )
    manager = MigrationManager(cl)
    path = manager.find_path("0001_a", "0002_b")
    assert path == []


@pytest.mark.unit
def test_find_path_branching() -> None:
    cl = Changelog(
        version=1,
        migrations=[
            Migration(name="0001_a"),
            Migration(name="0002_b", parents=("0001_a",)),
            Migration(name="0003_c", parents=("0001_a",)),
            Migration(name="0004_d", parents=("0003_c",)),
        ],
    )
    manager = MigrationManager(cl)
    path = manager.find_path("0001_a", "0004_d")
    assert path == ["0001_a", "0003_c", "0004_d"]


# --- manager.print_list / manager.print_dag tests ---


@pytest.mark.unit
def test_print_list() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_add.sql", parents=("0000_init.sql",)),
    ]
    changelog = Changelog(version=1, migrations=migrations)
    manager = MigrationManager(changelog)
    status_map: dict[str, MigrationStatus] = {
        "0000_init.sql": MigrationStatus.APPLIED,
        "0001_add.sql": MigrationStatus.NOT_APPLIED,
    }

    lines: list[str] = []
    with patch("migrateit.models.migration_manager.write_line", lines.append):
        manager.print_list(status_map)

    assert len(lines) == 2
    assert "0000_init.sql" in lines[0]
    assert "Applied" in lines[0]
    assert "0001_add.sql" in lines[1]
    assert "Not Applied" in lines[1]


@pytest.mark.unit
def test_print_dag() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=()),
        Migration(name="0001_add.sql", parents=("0000_init.sql",)),
        Migration(name="0002_two.sql", parents=("0000_init.sql", "0001_add.sql")),
    ]
    changelog = Changelog(version=1, migrations=migrations)
    manager = MigrationManager(changelog)
    status_map: dict[str, MigrationStatus] = {
        "0000_init.sql": MigrationStatus.APPLIED,
        "0001_add.sql": MigrationStatus.NOT_APPLIED,
        "0002_two.sql": MigrationStatus.NOT_APPLIED,
    }

    lines: list[str] = []
    with patch("migrateit.models.migration_manager.write_line", lines.append):
        manager.print_dag(status_map)

    assert len(lines) == 4
    assert "0000_init.sql" in lines[0]
    assert "└─ 0001_add.sql" in lines[1]
    assert "Applied" in lines[0]
    assert "Not Applied" in lines[1]


@pytest.mark.unit
def test_print_dag_no_children() -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=())
    changelog = Changelog(version=1, migrations=[m])
    manager = MigrationManager(changelog)
    status_map: dict[str, MigrationStatus] = {"0000_init.sql": MigrationStatus.APPLIED}

    lines: list[str] = []
    with patch("migrateit.models.migration_manager.write_line", lines.append):
        manager.print_dag(status_map)

    assert len(lines) == 1
    assert "0000_init.sql" in lines[0]
    assert "Applied" in lines[0]
