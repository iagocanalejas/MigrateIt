from pathlib import Path

import pytest

from migrateit.models.changelog import ChangelogFile, Migration
from migrateit.models.migration import MigrationStatus
from migrateit.tree import (
    ROLLBACK_SPLIT_TAG,
    build_migration_plan,
    build_migrations_tree,
    find_path,
    load_changelog_file,
    retrieve_migration_sqls,
    write_into_migration_file,
)

# --- retrieve_migration_sqls tests ---


def test_retrieve_sql_with_both_sections(temp_dir: Path) -> None:
    file_path = temp_dir / "test.sql"
    content = (
        "-- Migration 0001_test.sql\n"
        "-- Created on 2024-01-01\n\n"
        "CREATE TABLE test (id INT);\n\n"
        "-- Rollback migration\n\n"
        "DROP TABLE test;\n"
    )
    file_path.write_text(content)
    sql, rollback = retrieve_migration_sqls(file_path)
    assert sql == "CREATE TABLE test (id INT);"
    assert rollback == "DROP TABLE test;"


def test_retrieve_sql_only_forward(temp_dir: Path) -> None:
    file_path = temp_dir / "test.sql"
    content = "-- Migration 0001_test.sql\nSELECT 1;\n"
    file_path.write_text(content)
    sql, rollback = retrieve_migration_sqls(file_path)
    assert sql == "SELECT 1;"
    assert rollback is None


def test_retrieve_sql_empty_forward(temp_dir: Path) -> None:
    file_path = temp_dir / "test.sql"
    content = f"\n\n{ROLLBACK_SPLIT_TAG}\n\nDROP TABLE test;\n"
    file_path.write_text(content)
    sql, rollback = retrieve_migration_sqls(file_path)
    assert sql == ""
    assert rollback == "DROP TABLE test;"


def test_retrieve_sql_multiple_statements(temp_dir: Path) -> None:
    file_path = temp_dir / "test.sql"
    content = (
        "-- Migration\n"
        "CREATE TABLE a (id INT);\n"
        "CREATE TABLE b (id INT);\n\n"
        "-- Rollback migration\n\n"
        "DROP TABLE b; DROP TABLE a;"
    )
    file_path.write_text(content)
    sql, rollback = retrieve_migration_sqls(file_path)
    assert sql == "CREATE TABLE a (id INT);\nCREATE TABLE b (id INT);"
    assert rollback == "DROP TABLE b; DROP TABLE a;"


def test_retrieve_sql_nonexistent_file(temp_dir: Path) -> None:
    bad_path = temp_dir / "nonexistent.sql"
    with pytest.raises(ValueError):
        retrieve_migration_sqls(bad_path)


def test_retrieve_sql_non_sql_file(temp_dir: Path) -> None:
    bad_path = temp_dir / "test.txt"
    bad_path.write_text("SELECT 1;")
    with pytest.raises(ValueError):
        retrieve_migration_sqls(bad_path)


def test_retrieve_sql_no_rollback_tag_removes_comments(temp_dir: Path) -> None:
    file_path = temp_dir / "test.sql"
    content = "-- Migration 0001_test.sql\n-- Created on 2024-01-01\n\nSELECT 1;\n"
    file_path.write_text(content)
    sql, rollback = retrieve_migration_sqls(file_path)
    assert sql is not None
    assert rollback is None
    assert "-- Migration" not in sql
    assert "-- Created on" not in sql
    assert sql == "SELECT 1;"


# --- write_into_migration_file tests ---


def _make_migration_file(temp_dir: Path, filename: str = "0001_test.sql") -> Path:
    file_path = temp_dir / filename
    template = f"-- Migration {file_path.name}\n-- Created on 2024-01-01\n\n{ROLLBACK_SPLIT_TAG}"
    file_path.write_text(template)
    return file_path


def test_write_sql_and_rollback(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    write_into_migration_file(file_path, sql="CREATE TABLE test (id INT);", rollback="DROP TABLE test;")
    content = file_path.read_text()
    assert "CREATE TABLE test (id INT);" in content
    assert "DROP TABLE test;" in content


def test_write_sql_only(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    write_into_migration_file(file_path, sql="SELECT 1;", rollback=None)
    content = file_path.read_text()
    assert "SELECT 1;" in content


def test_write_rollback_only(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    write_into_migration_file(file_path, sql=None, rollback="SELECT 2;")
    content = file_path.read_text()
    assert "SELECT 2;" in content


def test_write_both_none_raises(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    with pytest.raises(ValueError):
        write_into_migration_file(file_path, sql=None, rollback=None)


def test_write_no_rollback_tag_raises(temp_dir: Path) -> None:
    bad_path = temp_dir / "bad.sql"
    bad_path.write_text("SELECT 1;")
    with pytest.raises(ValueError):
        write_into_migration_file(bad_path, sql="SELECT 1;", rollback=None)


def test_write_sql_strips_newlines(temp_dir: Path) -> None:
    file_path = _make_migration_file(temp_dir)
    write_into_migration_file(
        file_path,
        sql="\n\n  CREATE TABLE test (id INT);  \n\n",
        rollback="\n\n  DROP TABLE test;  \n\n",
    )
    content = file_path.read_text()
    assert "CREATE TABLE test (id INT);" in content
    assert "DROP TABLE test;" in content
    # Verify excessive blank lines are collapsed
    assert "\n\n\n\n" not in content


# --- build_migrations_tree tests ---


def test_build_tree_simple_chain() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_test.sql", parents=["0000_init.sql"]),
        Migration(name="0002_next.sql", parents=["0001_test.sql"]),
    ]
    changelog = ChangelogFile(version=1, migrations=migrations)
    tree = build_migrations_tree(changelog)
    assert list(tree.keys()) == ["0000_init.sql", "0001_test.sql", "0002_next.sql"]
    assert tree["0000_init.sql"] == [migrations[1]]
    assert tree["0001_test.sql"] == [migrations[2]]
    assert tree["0002_next.sql"] == []


def test_build_tree_with_multiple_parents() -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_branch_a.sql", parents=["0000_init.sql"])
    m3 = Migration(name="0002_branch_b.sql", parents=["0000_init.sql"])
    m4 = Migration(name="0003_merge.sql", parents=["0001_branch_a.sql", "0002_branch_b.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2, m3, m4])
    tree = build_migrations_tree(changelog)
    assert tree["0003_merge.sql"] == []
    assert m4 in tree["0001_branch_a.sql"]
    assert m4 in tree["0002_branch_b.sql"]


# --- find_path tests ---


def test_find_path_same_node() -> None:
    tree: dict[str, list[Migration]] = {"0001_a": []}
    path = find_path(tree, "0001_a", "0001_a")
    assert path == ["0001_a"]


def test_find_path_linear() -> None:
    tree = {
        "0001_a": [Migration(name="0002_b")],
        "0002_b": [Migration(name="0003_c")],
        "0003_c": [],
    }
    path = find_path(tree, "0001_a", "0003_c")
    assert path == ["0001_a", "0002_b", "0003_c"]


def test_find_path_no_path() -> None:
    tree: dict[str, list[Migration]] = {
        "0001_a": [],
        "0002_b": [],
    }
    path = find_path(tree, "0001_a", "0002_b")
    assert path == []


def test_find_path_branching() -> None:
    tree = {
        "0001_root": [
            Migration(name="0002_left"),
            Migration(name="0003_right"),
        ],
        "0002_left": [],
        "0003_right": [Migration(name="0004_deep")],
        "0004_deep": [],
    }
    path = find_path(tree, "0001_root", "0004_deep")
    assert path == ["0001_root", "0003_right", "0004_deep"]


def test_find_path_deeply_nested() -> None:
    tree = {
        "0001_a": [Migration(name="0002_b")],
        "0002_b": [Migration(name="0003_c")],
        "0003_c": [Migration(name="0004_d")],
        "0004_d": [],
    }
    path = find_path(tree, "0001_a", "0004_d")
    assert path == ["0001_a", "0002_b", "0003_c", "0004_d"]


# --- build_migration_plan tests (unique cases not in plan_builder_test.py) ---


def test_plan_empty_changelog(temp_dir: Path) -> None:
    empty_changelog = ChangelogFile(version=1, migrations=[], path=Path("changelog.json"))
    with pytest.raises(IndexError):
        build_migration_plan(empty_changelog, {}, {})


def test_plan_with_conflict_statuses(temp_dir: Path) -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_add_users.sql", parents=["0000_init.sql"])
    m3 = Migration(name="0002_add_orders.sql", parents=["0000_init.sql"])
    m4 = Migration(name="0003_merge.sql", parents=["0001_add_users.sql", "0002_add_orders.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2, m3, m4], path=temp_dir / "changelog.json")
    statuses = {n.name: MigrationStatus.CONFLICT for n in [m1, m2, m3, m4]}
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
    tree = build_migrations_tree(changelog)
    plan = build_migration_plan(changelog, tree, statuses)
    assert len(plan) == 4


# --- changelog file I/O tests (unique cases not in tree_utils_test.py) ---


def test_load_multiple_initial_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [
        Migration(name="0000_a.sql", initial=True, parents=[]),
        Migration(name="0001_b.sql", initial=True, parents=[]),
    ]
    cl = ChangelogFile(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError):
        load_changelog_file(path)


def test_load_initial_with_parents_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [Migration(name="0000_a.sql", initial=True, parents=["0001_b.sql"])]
    cl = ChangelogFile(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError):
        load_changelog_file(path)


def test_load_non_initial_without_parents_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [Migration(name="0000_a.sql", initial=True, parents=[])]
    cl = ChangelogFile(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    # Single initial migration is valid; adding a non-initial without parents
    migrations.append(Migration(name="0001_b.sql", parents=[]))
    cl.migrations = migrations
    path.write_text(cl.to_json())
    with pytest.raises(ValueError):
        load_changelog_file(path)
