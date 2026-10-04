from pathlib import Path

import pytest

from migrateit.models.changelog import ChangelogFile, SupportedDatabase, create_changelog_file, load_changelog_file
from migrateit.models.migration import Migration, MigrationStatus

# --- changelog.to_dict tests ---


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_changelog_file_to_dict(database: SupportedDatabase) -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=[])
    changelog = ChangelogFile(version=1, database=database, migrations=[m])
    d = changelog.to_dict()
    assert d["version"] == 1
    assert d["database"] == database.value
    assert len(d["migrations"]) == 1


# --- changelog.from_json tests ---


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_changelog_file_to_json(database: SupportedDatabase) -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=[])
    changelog = ChangelogFile(version=1, database=database, migrations=[m])
    json_str = changelog.to_json()
    assert "version" in json_str
    assert "0000_init.sql" in json_str


# --- changelog.exist_migration_by_name tests ---


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


# --- changelog.get_migration_by_name tests ---


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
    with pytest.raises(ValueError, match="not found"):
        changelog.get_migration_by_name("0002_nonexistent")


@pytest.mark.unit
def test_changelog_file_get_migration_by_name_abs_path() -> None:
    m = Migration(name="0001_test.sql", parents=["0000_init.sql"])
    changelog = ChangelogFile(version=1, migrations=[m])
    result = changelog.get_migration_by_name("/some/path/0001_test.sql")
    assert result.name == "0001_test.sql"


@pytest.mark.unit
def test_get_migration_by_name_prefix_collision_returns_first(temp_dir: Path) -> None:
    m1 = Migration(name="0001_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_test.sql", parents=["0000_init.sql"])

    changelog_path = temp_dir / "changelog.json"
    changelog_path.touch()

    changelog = ChangelogFile(version=1, migrations=[m1, m2], path=changelog_path)
    changelog.save()

    with pytest.raises(ValueError, match="duplicated index"):
        load_changelog_file(changelog_path)


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
def test_build_tree_duplicated_name_raises() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_test.sql", parents=["0000_init.sql"]),
        Migration(name="0001_test.sql", parents=["0000_init.sql"]),
    ]
    changelog = ChangelogFile(version=1, migrations=migrations)
    with pytest.raises(ValueError, match="duplicated"):
        changelog.migrations_tree


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


# --- build_migration_plan tests ---


@pytest.mark.unit
def test_build_plan_simple_forward() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_add_table.sql", parents=["0000_init.sql"]),
    ]
    changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = {"0000_init.sql": MigrationStatus.APPLIED, "0001_add_table.sql": MigrationStatus.NOT_APPLIED}

    plan = changelog.build_migration_plan(statuses)
    assert len(plan) == 1
    assert plan[0].name == "0001_add_table.sql"


@pytest.mark.unit
def test_build_plan_diamond_requeue_visited() -> None:
    """Diamond DAG with cross-parent dependency.

    Structure: A → B, A → C, B → C (cross-parent), B → D, C → D.

    When A is processed, B and C are enqueued.
    When B is processed, C tries to be enqueued again, but it's already queued.
    When C is processed, D is enqueued.
    D is processed.
    """
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_left.sql", parents=["0000_init.sql"]),
        Migration(name="0002_right.sql", parents=["0000_init.sql", "0001_left.sql"]),
        Migration(name="0003_merge.sql", parents=["0001_left.sql", "0002_right.sql"]),
    ]
    changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = dict.fromkeys([m.name for m in migrations], MigrationStatus.NOT_APPLIED)

    plan = changelog.build_migration_plan(statuses)
    assert [m.name for m in plan] == ["0000_init.sql", "0001_left.sql", "0002_right.sql", "0003_merge.sql"]


@pytest.mark.unit
def test_build_plan_unvisited_parents() -> None:
    """
    Structure: A → B, A → D, B → C, C → D

    When A is processed, B and D are enqueued.
    When B is processed, C is enqueued.
    When D is processed, C is not visited yet, so we skip and requeue it.
    When C is processed, D tries to be enqueued again, but it's already queued.
    D is processed.
    """
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_init.sql", parents=["0000_init.sql"]),
        Migration(name="0002_init.sql", parents=["0001_init.sql"]),
        Migration(name="0003_init.sql", parents=["0000_init.sql", "0002_init.sql"]),
    ]
    changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = dict.fromkeys([m.name for m in migrations], MigrationStatus.NOT_APPLIED)

    plan = changelog.build_migration_plan(statuses)
    assert [m.name for m in plan] == ["0000_init.sql", "0001_init.sql", "0002_init.sql", "0003_init.sql"]


@pytest.mark.unit
def test_build_plan_already_visited_neighbor() -> None:
    """
    Structure: A → B, A → C, B → C
    is_bottom_up=True

    When C is processed, it's parents A and B are enqueued.
    A is processed.
    When B is processed, A tries to be enqueued again, but it's already queued.
    """
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_init.sql", parents=["0000_init.sql"]),
        Migration(name="0002_init.sql", parents=["0001_init.sql", "0000_init.sql"]),
    ]
    changelog = ChangelogFile(version=1, migrations=migrations)
    statuses = dict.fromkeys([m.name for m in migrations], MigrationStatus.NOT_APPLIED)

    plan = changelog.build_migration_plan(statuses_map=statuses, target_migration=migrations[2])
    assert [m.name for m in plan] == ["0001_init.sql", "0000_init.sql", "0002_init.sql"]


@pytest.mark.unit
def test_build_plan_rollback_applied() -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_add.sql", parents=["0000_init.sql"])
    m3 = Migration(name="0002_more.sql", parents=["0001_add.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2, m3])
    statuses: dict[str, MigrationStatus] = {
        "0000_init.sql": MigrationStatus.APPLIED,
        "0001_add.sql": MigrationStatus.APPLIED,
        "0002_more.sql": MigrationStatus.NOT_APPLIED,
    }
    # Rollback from 0001_add.sql — BFS visits [0001, 0000],
    # reversed to [0000, 0001], filtered to only APPLIED → [0001]
    plan = changelog.build_migration_plan(statuses, m2, is_rollback=True)
    assert len(plan) == 1
    assert plan[0].name == "0001_add.sql"


@pytest.mark.unit
def test_build_plan_bottom_up() -> None:
    m1 = Migration(name="0000_init.sql", initial=True, parents=[])
    m2 = Migration(name="0001_add.sql", parents=["0000_init.sql"])
    changelog = ChangelogFile(version=1, migrations=[m1, m2])
    statuses: dict[str, MigrationStatus] = {
        "0000_init.sql": MigrationStatus.APPLIED,
        "0001_add.sql": MigrationStatus.NOT_APPLIED,
    }
    plan = changelog.build_migration_plan(statuses, m2, is_rollback=False)
    # Bottom-up BFS visits [0001, 0000], reversed to [0000, 0001],
    # filtered to NOT_APPLIED → [0001]
    assert len(plan) == 1
    assert plan[0].name == "0001_add.sql"


@pytest.mark.unit
def test_build_plan_rollback_no_target() -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=[])
    changelog = ChangelogFile(version=1, migrations=[m])
    statuses: dict[str, MigrationStatus] = {"0000_init.sql": MigrationStatus.APPLIED}
    with pytest.raises(ValueError, match="Target migration is required for rollback"):
        changelog.build_migration_plan(statuses, is_rollback=True)


# --- save_changelog_file tests ---


@pytest.mark.unit
def test_save_changelog_file(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    path.touch()
    cl = ChangelogFile(version=2, path=path)
    cl.save()
    assert '"version": 2' in path.read_text()


@pytest.mark.unit
def test_save_changelog_file_not_exists(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    cl = ChangelogFile(version=1, path=path)
    with pytest.raises(FileNotFoundError):
        cl.save()


# --- create_new_migration tests ---


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_create_new_migration_success(temp_dir: Path, database: SupportedDatabase) -> None:
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(exist_ok=True)
    path = temp_dir / "changelog.json"
    cl = create_changelog_file(path, database)

    cl.create_new_migration(migrations_dir, "init")
    created_files = sorted(migrations_dir.iterdir())
    assert len(created_files) == 1
    assert created_files[0].name == "0000_init.sql"

    migrations = load_changelog_file(path)
    assert len(migrations.migrations) == 1
    assert migrations.migrations[0].name.endswith("init.sql")


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_create_new_migration_with_dependencies(temp_dir: Path, database: SupportedDatabase) -> None:
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(exist_ok=True)
    path = temp_dir / "changelog.json"
    cl = create_changelog_file(path, database)

    cl.create_new_migration(migrations_dir, "init")
    cl.create_new_migration(migrations_dir, "add_users", dependencies=["0000"])
    created_files = sorted(migrations_dir.iterdir())
    assert len(created_files) == 2
    assert created_files[0].name == "0000_init.sql"
    assert created_files[1].name == "0001_add_users.sql"

    migrations = load_changelog_file(path)
    assert len(migrations.migrations) == 2
    assert "init" in migrations.migrations[1].parents[0]


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_create_new_migration_invalid_name(temp_dir: Path, database: SupportedDatabase) -> None:
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(exist_ok=True)
    path = temp_dir / "changelog.json"
    cl = create_changelog_file(path, database)

    with pytest.raises(ValueError, match="not a valid identifier"):
        cl.create_new_migration(migrations_dir, "123-bad-name")

    with pytest.raises(ValueError, match="not a valid identifier"):
        cl.create_new_migration(migrations_dir, "")


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_create_new_migration_filename_collides_with_disk_file(temp_dir: Path, database: SupportedDatabase) -> None:
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(exist_ok=True)
    path = temp_dir / "changelog.json"

    (migrations_dir / "0000_init.sql").touch()

    cl = create_changelog_file(path, database)

    with pytest.raises(FileExistsError, match="already exists"):
        cl.create_new_migration(migrations_dir, "new")


# --- create_changelog_file tests ---


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_create_changelog_file(temp_dir: Path, database: SupportedDatabase) -> None:
    path = temp_dir / "changelog.json"
    cl = create_changelog_file(path, database)
    assert path.exists()
    assert cl.version == 1
    assert cl.database == database
    assert cl.path == path


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_create_changelog_file_invalid_extension(temp_dir: Path, database: SupportedDatabase) -> None:
    bad_path = temp_dir / "migrations.txt"
    with pytest.raises(ValueError, match="must be a JSON file"):
        create_changelog_file(bad_path, database)


# --- load_changelog_file tests ---


@pytest.mark.unit
def test_load_changelog_file_not_exists(temp_dir: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_changelog_file(temp_dir / "changelog.json")


@pytest.mark.unit
def test_load_changelog_file_valid(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    cl = ChangelogFile(version=1, migrations=[], path=path)
    path.write_text(cl.to_json())
    loaded = load_changelog_file(path)
    assert loaded.version == 1


@pytest.mark.unit
def test_load_changelog_file_multiple_initial_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [
        Migration(name="0000_a.sql", initial=True, parents=[]),
        Migration(name="0001_b.sql", initial=True, parents=[]),
    ]
    cl = ChangelogFile(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="exactly one initial migration"):
        load_changelog_file(path)


@pytest.mark.unit
def test_load_changelog_file_initial_with_parents_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [Migration(name="0000_a.sql", initial=True, parents=["0001_b.sql"])]
    cl = ChangelogFile(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="cannot have parents"):
        load_changelog_file(path)


@pytest.mark.unit
def test_load_changelog_file_non_initial_without_parents_raises(temp_dir: Path) -> None:
    path = temp_dir / "changelog.json"
    migrations = [Migration(name="0000_a.sql", initial=True, parents=[])]
    cl = ChangelogFile(version=1, migrations=migrations, path=path)
    path.write_text(cl.to_json())
    # Single initial migration is valid; adding a non-initial without parents
    migrations.append(Migration(name="0001_b.sql", parents=[]))
    cl.migrations = migrations
    path.write_text(cl.to_json())
    with pytest.raises(ValueError, match="must have parents"):
        load_changelog_file(path)


# --- find_path tests ---


@pytest.mark.unit
def test_find_path_same_node() -> None:
    cl = ChangelogFile(version=1, migrations=[Migration(name="0001_a")])
    path = cl.find_path("0001_a", "0001_a")
    assert path == ["0001_a"]


@pytest.mark.unit
def test_find_path_linear() -> None:
    cl = ChangelogFile(
        version=1,
        migrations=[
            Migration(name="0001_a"),
            Migration(name="0002_b", parents=["0001_a"]),
            Migration(name="0003_c", parents=["0002_b"]),
        ],
    )
    path = cl.find_path("0001_a", "0003_c")
    assert path == ["0001_a", "0002_b", "0003_c"]


@pytest.mark.unit
def test_find_path_no_path() -> None:
    cl = ChangelogFile(
        version=1,
        migrations=[
            Migration(name="0001_a"),
            Migration(name="0002_b"),
        ],
    )
    path = cl.find_path("0001_a", "0002_b")
    assert path == []


@pytest.mark.unit
def test_find_path_branching() -> None:
    cl = ChangelogFile(
        version=1,
        migrations=[
            Migration(name="0001_a"),
            Migration(name="0002_b", parents=["0001_a"]),
            Migration(name="0003_c", parents=["0001_a"]),
            Migration(name="0004_d", parents=["0003_c"]),
        ],
    )
    path = cl.find_path("0001_a", "0004_d")
    assert path == ["0001_a", "0003_c", "0004_d"]


# --- print_list / print_dag tests ---


@pytest.mark.unit
def test_print_list() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_add.sql", parents=["0000_init.sql"]),
    ]
    changelog = ChangelogFile(version=1, migrations=migrations)
    status_map: dict[str, MigrationStatus] = {
        "0000_init.sql": MigrationStatus.APPLIED,
        "0001_add.sql": MigrationStatus.NOT_APPLIED,
    }
    changelog.print_list(status_map)


@pytest.mark.unit
def test_print_dag() -> None:
    migrations = [
        Migration(name="0000_init.sql", initial=True, parents=[]),
        Migration(name="0001_add.sql", parents=["0000_init.sql"]),
    ]
    changelog = ChangelogFile(version=1, migrations=migrations)
    status_map: dict[str, MigrationStatus] = {
        "0000_init.sql": MigrationStatus.APPLIED,
        "0001_add.sql": MigrationStatus.NOT_APPLIED,
    }
    changelog.print_dag(status_map)


@pytest.mark.unit
def test_print_dag_no_children() -> None:
    m = Migration(name="0000_init.sql", initial=True, parents=[])
    changelog = ChangelogFile(version=1, migrations=[m])
    status_map: dict[str, MigrationStatus] = {"0000_init.sql": MigrationStatus.APPLIED}
    changelog.print_dag(status_map)
