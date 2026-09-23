from collections import OrderedDict
from pathlib import Path

import pytest

from migrateit.models.changelog import ChangelogFile
from migrateit.models.migration import Migration, MigrationStatus
from migrateit.tree import build_migration_plan


def _make_plan_setup(
    temp_dir: Path,
) -> tuple[ChangelogFile, Migration, Migration, Migration, Migration, Migration, dict[str, list[Migration]]]:
    m1 = Migration(name="0001_init.sql", initial=True, parents=[])
    m2 = Migration(name="0002_add_users.sql", parents=["0001_init.sql"])
    m3 = Migration(name="0003_add_orders.sql", parents=["0001_init.sql"])
    m4 = Migration(
        name="0004_add_queries.sql",
        parents=["0002_add_users.sql", "0003_add_orders.sql"],
    )
    m5 = Migration(name="0005_add_rows.sql", parents=["0004_add_queries.sql"])

    migrations = [m1, m2, m3, m4, m5]
    changelog = ChangelogFile(version=1, migrations=migrations, path=temp_dir / "changelog.json")

    migration_tree: dict[str, list[Migration]] = OrderedDict(
        {
            "0001_init.sql": [m2, m3],
            "0002_add_users.sql": [m4],
            "0003_add_orders.sql": [m4],
            "0004_add_queries.sql": [m5],
            "0005_add_rows.sql": [],
        }
    )
    return changelog, m1, m2, m3, m4, m5, migration_tree


# --- Forward plan tests ---


def test_plan_applies_unapplied_migrations(temp_dir: Path) -> None:
    changelog, m1, m2, m3, m4, m5, migration_tree = _make_plan_setup(temp_dir)
    statuses = {
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_add_users.sql": MigrationStatus.NOT_APPLIED,
        "0003_add_orders.sql": MigrationStatus.NOT_APPLIED,
        "0004_add_queries.sql": MigrationStatus.NOT_APPLIED,
        "0005_add_rows.sql": MigrationStatus.NOT_APPLIED,
    }

    plan = build_migration_plan(changelog, migration_tree, statuses)
    assert [m.name for m in plan] == [
        "0002_add_users.sql",
        "0003_add_orders.sql",
        "0004_add_queries.sql",
        "0005_add_rows.sql",
    ]


def test_plan_all_applied_returns_empty(temp_dir: Path) -> None:
    changelog, m1, m2, m3, m4, m5, migration_tree = _make_plan_setup(temp_dir)
    statuses = {
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_add_users.sql": MigrationStatus.APPLIED,
        "0003_add_orders.sql": MigrationStatus.APPLIED,
        "0004_add_queries.sql": MigrationStatus.APPLIED,
        "0005_add_rows.sql": MigrationStatus.APPLIED,
    }

    plan = build_migration_plan(changelog, migration_tree, statuses)
    assert plan == []


def test_plan_single_migration_applied(temp_dir: Path) -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_add_users.sql", parents=["0000_init.sql"])
    m3 = Migration(name="0002_add_orders.sql", parents=["0000_init.sql"])
    m4 = Migration(name="0003_merge.sql", parents=["0001_add_users.sql", "0002_add_orders.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2, m3, m4], path=temp_dir / "changelog.json")
    statuses = {n.name: MigrationStatus.APPLIED for n in [m1, m2, m3, m4]}
    from migrateit.tree import build_migrations_tree

    tree = build_migrations_tree(changelog)
    plan = build_migration_plan(changelog, tree, statuses)
    assert plan == []


def test_plan_only_init_not_applied(temp_dir: Path) -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_add_users.sql", parents=["0000_init.sql"])
    m3 = Migration(name="0002_add_orders.sql", parents=["0000_init.sql"])
    m4 = Migration(name="0003_merge.sql", parents=["0001_add_users.sql", "0002_add_orders.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2, m3, m4], path=temp_dir / "changelog.json")
    statuses = {
        "0000_init.sql": MigrationStatus.NOT_APPLIED,
        "0001_add_users.sql": MigrationStatus.APPLIED,
        "0002_add_orders.sql": MigrationStatus.APPLIED,
        "0003_merge.sql": MigrationStatus.APPLIED,
    }
    from migrateit.tree import build_migrations_tree

    tree = build_migrations_tree(changelog)
    plan = build_migration_plan(changelog, tree, statuses)
    assert len(plan) == 1
    assert plan[0].name == "0000_init.sql"


def test_plan_empty_changelog(temp_dir: Path) -> None:
    empty_changelog = ChangelogFile(version=1, migrations=[], path=Path("changelog.json"))
    with pytest.raises(IndexError):
        build_migration_plan(
            empty_changelog,
            {},
            {},
        )


def test_plan_with_conflict_statuses(temp_dir: Path) -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_add_users.sql", parents=["0000_init.sql"])
    m3 = Migration(name="0002_add_orders.sql", parents=["0000_init.sql"])
    m4 = Migration(name="0003_merge.sql", parents=["0001_add_users.sql", "0002_add_orders.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2, m3, m4], path=temp_dir / "changelog.json")
    statuses = {n.name: MigrationStatus.CONFLICT for n in [m1, m2, m3, m4]}
    from migrateit.tree import build_migrations_tree

    tree = build_migrations_tree(changelog)
    plan = build_migration_plan(changelog, tree, statuses)
    assert len(plan) == 4


def test_plan_with_removed_statuses(temp_dir: Path) -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_add_users.sql", parents=["0000_init.sql"])
    m3 = Migration(name="0002_add_orders.sql", parents=["0000_init.sql"])
    m4 = Migration(name="0003_merge.sql", parents=["0001_add_users.sql", "0002_add_orders.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2, m3, m4], path=temp_dir / "changelog.json")
    statuses = {n.name: MigrationStatus.REMOVED for n in [m1, m2, m3, m4]}
    from migrateit.tree import build_migrations_tree

    tree = build_migrations_tree(changelog)
    plan = build_migration_plan(changelog, tree, statuses)
    assert len(plan) == 4


# --- Bottom-up plan tests ---


def test_bottom_up_plan(temp_dir: Path) -> None:
    changelog, m1, m2, m3, m4, m5, migration_tree = _make_plan_setup(temp_dir)
    statuses = {
        "0001_init.sql": MigrationStatus.NOT_APPLIED,
        "0002_add_users.sql": MigrationStatus.NOT_APPLIED,
        "0003_add_orders.sql": MigrationStatus.NOT_APPLIED,
        "0004_add_queries.sql": MigrationStatus.NOT_APPLIED,
        "0005_add_rows.sql": MigrationStatus.NOT_APPLIED,
    }

    plan = build_migration_plan(
        changelog,
        migration_tree,
        statuses,
        target_migration=m4,
        is_rollback=False,
    )

    assert [m.name for m in plan] == [
        "0001_init.sql",
        "0002_add_users.sql",
        "0003_add_orders.sql",
        "0004_add_queries.sql",
    ]


# --- Rollback plan tests ---


def test_rollback_plan(temp_dir: Path) -> None:
    changelog, m1, m2, m3, m4, m5, migration_tree = _make_plan_setup(temp_dir)
    statuses = {
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_add_users.sql": MigrationStatus.APPLIED,
        "0003_add_orders.sql": MigrationStatus.NOT_APPLIED,
        "0004_add_queries.sql": MigrationStatus.APPLIED,
        "0005_add_rows.sql": MigrationStatus.APPLIED,
    }

    plan = build_migration_plan(
        changelog,
        migration_tree,
        statuses,
        target_migration=m2,
        is_rollback=True,
    )

    assert [m.name for m in plan] == [
        "0005_add_rows.sql",
        "0004_add_queries.sql",
        "0002_add_users.sql",
    ]


def test_rollback_skips_unapplied(temp_dir: Path) -> None:
    changelog, m1, m2, m3, m4, m5, migration_tree = _make_plan_setup(temp_dir)
    statuses = {
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_add_users.sql": MigrationStatus.NOT_APPLIED,
        "0003_add_orders.sql": MigrationStatus.NOT_APPLIED,
        "0004_add_queries.sql": MigrationStatus.NOT_APPLIED,
        "0005_add_rows.sql": MigrationStatus.NOT_APPLIED,
    }

    plan = build_migration_plan(
        changelog,
        migration_tree,
        statuses,
        target_migration=m1,
        is_rollback=True,
    )

    assert [m.name for m in plan] == ["0001_init.sql"]


def test_rollback_to_init(temp_dir: Path) -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_add_users.sql", parents=["0000_init.sql"])
    m3 = Migration(name="0002_add_orders.sql", parents=["0000_init.sql"])
    m4 = Migration(name="0003_merge.sql", parents=["0001_add_users.sql", "0002_add_orders.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2, m3, m4], path=temp_dir / "changelog.json")
    statuses = {n.name: MigrationStatus.APPLIED for n in [m1, m2, m3, m4]}
    from migrateit.tree import build_migrations_tree

    tree = build_migrations_tree(changelog)
    plan = build_migration_plan(
        changelog,
        tree,
        statuses,
        target_migration=m1,
        is_rollback=True,
    )
    # All are APPLIED and reachable from init (it's the root)
    assert len(plan) == 4
    # Plan should be in reverse order (last migration first)
    assert plan[0].name == "0003_merge.sql"
    assert plan[3].name == "0000_init.sql"


def test_raises_if_target_missing_in_rollback(temp_dir: Path) -> None:
    changelog, m1, m2, m3, m4, m5, migration_tree = _make_plan_setup(temp_dir)
    statuses = {
        "0001_init.sql": MigrationStatus.APPLIED,
    }

    with pytest.raises(ValueError):
        build_migration_plan(
            changelog,
            migration_tree,
            statuses,
            target_migration=None,
            is_rollback=True,
        )
