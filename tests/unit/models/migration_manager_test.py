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
        manager.create_new_migration(name="invalid-name")


@pytest.mark.unit
def test_create_new_migration_dependency_not_found(temp_dir: Path) -> None:
    path = temp_dir / "migrations"
    path.mkdir(parents=True, exist_ok=True)
    changelog = Changelog(version=1, migrations=[Migration(name="0000_init.sql", initial=True, parents=())])
    manager = MigrationManager(changelog)
    with pytest.raises(ValueError, match="Some dependencies"):
        manager.create_new_migration(name="test", dependencies=("9999_missing",))


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
            manager.create_new_migration(name="test", dependencies=("0000_init.sql",))


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
        result = manager._get_next_migration_path("new_name")

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
            manager._get_next_migration_path("new_name")


# --- manager.build_migration_plan tests ---


@pytest.mark.unit
@pytest.mark.parametrize(
    ("migrations", "statuses", "target", "is_rollback", "expected"),
    [
        pytest.param(
            [
                Migration(name="0000_init.sql", initial=True, parents=()),
                Migration(name="0001_add_table.sql", parents=("0000_init.sql",)),
            ],
            {"0000_init.sql": MigrationStatus.APPLIED, "0001_add_table.sql": MigrationStatus.NOT_APPLIED},
            None,
            False,
            ["0001_add_table.sql"],
            id="simple_forward",
        ),
        pytest.param(
            [
                Migration(name="0000_init.sql", initial=True, parents=()),
                Migration(name="0001_left.sql", parents=("0000_init.sql",)),
                Migration(name="0002_right.sql", parents=("0000_init.sql", "0001_left.sql")),
                Migration(name="0003_merge.sql", parents=("0001_left.sql", "0002_right.sql")),
            ],
            dict.fromkeys(
                ["0000_init.sql", "0001_left.sql", "0002_right.sql", "0003_merge.sql"], MigrationStatus.NOT_APPLIED
            ),
            None,
            False,
            ["0000_init.sql", "0001_left.sql", "0002_right.sql", "0003_merge.sql"],
            id="diamond_requeue_visited",
        ),
        pytest.param(
            [
                Migration(name="0000_init.sql", initial=True, parents=()),
                Migration(name="0001_init.sql", parents=("0000_init.sql",)),
                Migration(name="0002_init.sql", parents=("0001_init.sql",)),
                Migration(name="0003_init.sql", parents=("0000_init.sql", "0002_init.sql")),
            ],
            dict.fromkeys(
                ["0000_init.sql", "0001_init.sql", "0002_init.sql", "0003_init.sql"], MigrationStatus.NOT_APPLIED
            ),
            None,
            False,
            ["0000_init.sql", "0001_init.sql", "0002_init.sql", "0003_init.sql"],
            id="unvisited_parents",
        ),
        pytest.param(
            [
                Migration(name="0000_init.sql", initial=True, parents=()),
                Migration(name="0001_init.sql", parents=("0000_init.sql",)),
                Migration(name="0002_init.sql", parents=("0001_init.sql", "0000_init.sql")),
            ],
            dict.fromkeys(["0000_init.sql", "0001_init.sql", "0002_init.sql"], MigrationStatus.NOT_APPLIED),
            "0002_init.sql",
            False,
            ["0001_init.sql", "0000_init.sql", "0002_init.sql"],
            id="already_visited_neighbor",
        ),
        pytest.param(
            [
                Migration(name="0000_init.sql", initial=True, parents=()),
                Migration(name="0001_add.sql", parents=("0000_init.sql",)),
                Migration(name="0002_more.sql", parents=("0001_add.sql",)),
            ],
            {
                "0000_init.sql": MigrationStatus.APPLIED,
                "0001_add.sql": MigrationStatus.APPLIED,
                "0002_more.sql": MigrationStatus.NOT_APPLIED,
            },
            "0001_add.sql",
            True,
            ["0001_add.sql"],
            id="rollback_applied",
        ),
        pytest.param(
            [
                Migration(name="0000_init.sql", initial=True, parents=()),
                Migration(name="0001_add.sql", parents=("0000_init.sql",)),
                Migration(name="0002_more.sql", parents=("0001_add.sql",)),
            ],
            {
                "0000_init.sql": MigrationStatus.APPLIED,
                "0001_add.sql": MigrationStatus.APPLIED,
                "0002_more.sql": MigrationStatus.NOT_APPLIED,
            },
            "0000_init.sql",
            True,
            ["0001_add.sql", "0000_init.sql"],
            id="rollback_root",
        ),
        pytest.param(
            [
                Migration(name="0000_init.sql", initial=True, parents=()),
                Migration(name="0001_add.sql", parents=("0000_init.sql",)),
            ],
            {
                "0000_init.sql": MigrationStatus.APPLIED,
                "0001_add.sql": MigrationStatus.NOT_APPLIED,
            },
            "0001_add.sql",
            False,
            ["0001_add.sql"],
            id="bottom_up",
        ),
        pytest.param(
            [Migration(name="0000_init.sql", initial=True, parents=())],
            {"0000_init.sql": MigrationStatus.APPLIED},
            None,
            True,
            ["0000_init.sql"],
            id="rollback_no_target",
        ),
    ],
)
def test_build_plan(
    migrations: list[Migration],
    statuses: dict[str, MigrationStatus],
    target: str | None,
    is_rollback: bool,
    expected: list[str],
) -> None:
    changelog = Changelog(version=1, migrations=migrations)
    manager = MigrationManager(changelog)
    target_migration = next(m for m in migrations if m.name == target) if target else None
    plan = manager.build_migration_plan(statuses, target_migration, is_rollback=is_rollback)
    assert [m.name for m in plan] == expected


# --- manager.find_path tests ---


@pytest.mark.unit
@pytest.mark.parametrize(
    ("migrations", "start", "end", "expected"),
    [
        pytest.param(
            [Migration(name="0001_a")],
            "0001_a",
            "0001_a",
            ["0001_a"],
            id="same_node",
        ),
        pytest.param(
            [
                Migration(name="0001_a"),
                Migration(name="0002_b", parents=("0001_a",)),
                Migration(name="0003_c", parents=("0002_b",)),
            ],
            "0001_a",
            "0003_c",
            ["0001_a", "0002_b", "0003_c"],
            id="linear",
        ),
        pytest.param(
            [
                Migration(name="0001_a"),
                Migration(name="0002_b"),
            ],
            "0001_a",
            "0002_b",
            [],
            id="no_path",
        ),
        pytest.param(
            [
                Migration(name="0001_a"),
                Migration(name="0002_b", parents=("0001_a",)),
                Migration(name="0003_c", parents=("0001_a",)),
                Migration(name="0004_d", parents=("0003_c",)),
            ],
            "0001_a",
            "0004_d",
            ["0001_a", "0003_c", "0004_d"],
            id="branching",
        ),
    ],
)
def test_find_path(migrations: list[Migration], start: str, end: str, expected: list[str]) -> None:
    cl = Changelog(version=1, migrations=migrations)
    manager = MigrationManager(cl)
    path = manager.find_path(start, end)
    assert path == expected


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
