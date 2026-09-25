import os
import sqlite3
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest

from migrateit.cli import cmd_init, cmd_run
from migrateit.clients.sqlite import SqliteClient
from migrateit.models import MigrateItConfig
from migrateit.models.changelog import ChangelogFile, SupportedDatabase
from migrateit.models.migration import Migration, MigrationStatus
from migrateit.tree import (
    ROLLBACK_SPLIT_TAG,
    create_changelog_file,
    create_new_migration,
    load_changelog_file,
    save_changelog_file,
)

TEST_MIGRATIONS_TABLE = "migrations"


@pytest.fixture()
def conn() -> Generator[sqlite3.Connection]:
    """Create an in-memory SQLite connection for tests."""
    conn = sqlite3.connect(":memory:")
    conn.isolation_level = "DEFERRED"
    yield conn
    conn.close()


@pytest.fixture()
def client(
    conn: sqlite3.Connection,
    temp_dir: Path,
) -> Generator[SqliteClient]:
    """Create a SqliteClient with a clean test table and initial migration."""
    migrations_dir = temp_dir / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    changelog = create_changelog_file(temp_dir / "changelog.json", SupportedDatabase.SQLITE)

    create_new_migration(changelog=changelog, migrations_dir=migrations_dir, name="migrateit")
    config = MigrateItConfig(
        table_name=TEST_MIGRATIONS_TABLE,
        migrations_dir=migrations_dir,
        changelog=changelog,
    )
    client = SqliteClient(connection=conn, config=config)
    sql, _ = SqliteClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    conn.executescript(sql)
    yield client


def create_migration_file(
    migrations_dir: Path,
    filename: str,
    sql: str | None = None,
    rollback_sql: str | None = None,
) -> str:
    path = os.path.join(migrations_dir, filename)
    with open(path, "w") as f:
        f.write(sql or f"-- Migration {filename}\n")
        f.write(f"{ROLLBACK_SPLIT_TAG}")
        if rollback_sql:
            f.write(f"\n\n{rollback_sql}")
    return path


def test_sqlite_create_migrations_table(conn: sqlite3.Connection, temp_dir: Path) -> None:
    """Test that the migrations table is created with correct schema."""
    del temp_dir  # unused
    table_name = "test_migrations"
    sql, rollback = SqliteClient.create_migrations_table_str(table_name)
    conn.executescript(sql)

    cursor = conn.execute("PRAGMA table_info(test_migrations)")
    columns = {row[1]: row[2] for row in cursor.fetchall()}
    assert "id" in columns
    assert "migration_name" in columns
    assert "applied_at" in columns
    assert "change_hash" in columns
    assert "squashed" in columns

    assert rollback is not None


def test_sqlite_is_migrations_table_created(client: SqliteClient) -> None:
    """Test detection of migrations table existence."""
    assert client.is_migrations_table_created() is True

    client.connection.execute("DROP TABLE migrations")

    assert client.is_migrations_table_created() is False


def test_sqlite_is_migration_applied(client: SqliteClient) -> None:
    """Test checking if a specific migration is applied."""
    migration = Migration(name="0001_test.sql")
    assert client.is_migration_applied(migration) is False

    client.connection.execute(
        f"INSERT INTO {client.table_name} (migration_name, change_hash) VALUES (?, ?);",
        ("0001_test.sql", "abc123"),
    )

    assert client.is_migration_applied(migration) is True


def test_sqlite_retrieve_statuses_no_table(temp_dir: Path) -> None:
    """Test retrieving statuses when no migrations table exists."""
    from migrateit.tree import create_new_migration

    conn = sqlite3.connect(":memory:")
    try:
        conn.isolation_level = "DEFERRED"
        migrations_dir = temp_dir / "migrations"
        migrations_dir.mkdir(parents=True, exist_ok=True)
        changelog = create_changelog_file(temp_dir / "changelog.json", SupportedDatabase.SQLITE)
        create_new_migration(changelog=changelog, migrations_dir=migrations_dir, name="migrateit")
        config = MigrateItConfig(
            table_name=TEST_MIGRATIONS_TABLE,
            migrations_dir=migrations_dir,
            changelog=changelog,
        )
        client = SqliteClient(connection=conn, config=config)
        statuses = client.retrieve_migration_statuses()
        assert len(statuses) == 1
        assert statuses["0000_migrateit.sql"] == MigrationStatus.NOT_APPLIED
    finally:
        conn.close()


def test_sqlite_apply_and_status(client: SqliteClient, temp_dir: Path) -> None:
    """Test applying a migration and checking status."""
    # Create a migration file
    migration_file = temp_dir / "migrations" / "0001_create_users.sql"
    migration_file.parent.mkdir(parents=True, exist_ok=True)
    migration_file.write_text(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n\n"
        + ROLLBACK_SPLIT_TAG
        + "\nDROP TABLE users;"
    )

    # Update changelog
    client.changelog.migrations.append(
        Migration(name="0001_create_users.sql", initial=False, parents=["0000_migrateit.sql"])
    )

    # Apply migration
    migration = client.changelog.migrations[1]
    client.apply_migration(migration)
    client.connection.commit()

    # Verify status
    statuses = client.retrieve_migration_statuses()
    assert statuses["0001_create_users.sql"] == MigrationStatus.APPLIED

    # Verify table was created
    cursor = client.connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    assert cursor.fetchone() is not None


def test_sqlite_rollback(client: SqliteClient, temp_dir: Path) -> None:
    """Test rolling back a migration."""
    # Create migration
    migration_file = temp_dir / "migrations" / "0001_create_items.sql"
    migration_file.parent.mkdir(parents=True, exist_ok=True)
    migration_file.write_text(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, title TEXT);\n\n" + ROLLBACK_SPLIT_TAG + "\nDROP TABLE items;"
    )

    client.changelog.migrations.append(
        Migration(name="0001_create_items.sql", initial=False, parents=["0000_migrateit.sql"])
    )

    # Apply
    migration = client.changelog.migrations[1]
    client.apply_migration(migration)
    client.connection.commit()

    # Verify applied
    cursor = client.connection.execute("SELECT migration_name FROM migrations")
    names = {row[0] for row in cursor.fetchall()}
    assert "0001_create_items.sql" in names

    # Rollback
    client.apply_migration(migration, is_rollback=True)
    client.connection.commit()

    # Verify rolled back
    cursor = client.connection.execute("SELECT migration_name FROM migrations")
    names = {row[0] for row in cursor.fetchall()}
    assert "0001_create_items.sql" not in names


def test_sqlite_update_hash(client: SqliteClient, temp_dir: Path) -> None:
    """Test updating migration hash."""
    # Create migration
    migration_file = temp_dir / "migrations" / "0001_test.sql"
    migration_file.parent.mkdir(parents=True, exist_ok=True)
    migration_file.write_text("SELECT 1;\n\n" + ROLLBACK_SPLIT_TAG + "\nSELECT 1;")

    client.changelog.migrations.append(Migration(name="0001_test.sql", initial=False, parents=["0000_migrateit.sql"]))

    client.update_migration_hash(client.changelog.migrations[1])
    client.connection.commit()

    # Verify hash stored
    cursor = client.connection.execute(
        f"SELECT change_hash FROM {client.table_name} WHERE migration_name='0001_test.sql'"
    )
    row = cursor.fetchone()
    assert row is not None
    assert len(row[0]) == 64  # SHA-256 hex digest


def test_sqlite_squash(client: SqliteClient, temp_dir: Path) -> None:
    """Test squashing migrations."""
    # Create two migrations
    for i, name in enumerate(["0001_first.sql", "0002_second.sql"]):
        f = temp_dir / "migrations" / name
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(f"SELECT {i};\n\n" + ROLLBACK_SPLIT_TAG + f"\nSELECT {i};")
        client.changelog.migrations.append(Migration(name=name, initial=False, parents=["0000_migrateit.sql"]))

    # Apply both
    for m in client.changelog.migrations[1:]:
        client.apply_migration(m)
    client.connection.commit()

    # Squash
    new_migration = Migration(name="0003_squashed_0001_0002.sql", initial=False, parents=["0000_migrateit.sql"])
    client.squash_migrations(["0001_first.sql", "0002_second.sql"], new_migration)
    client.connection.commit()

    # Verify squashed
    cursor = client.connection.execute(f"SELECT migration_name, squashed FROM {client.table_name}")
    rows = {row[0]: row[1] for row in cursor.fetchall()}
    assert rows.get("0001_first.sql") == 1
    assert rows.get("0002_second.sql") == 1
    assert "0003_squashed_0001_0002.sql" in rows


def test_sqlite_validate_sql_syntax(client: SqliteClient, temp_dir: Path) -> None:
    """Test SQL syntax validation."""
    # Valid SQL should return None
    migration_file = temp_dir / "migrations" / "0001_valid.sql"
    migration_file.parent.mkdir(parents=True, exist_ok=True)
    migration_file.write_text("CREATE TABLE t (id INTEGER PRIMARY KEY);\n\n" + ROLLBACK_SPLIT_TAG + "\nDROP TABLE t;")

    client.changelog.migrations.append(Migration(name="0001_valid.sql", initial=False, parents=["0000_migrateit.sql"]))
    assert client.validate_sql_syntax(client.changelog.migrations[1]) is None

    # Invalid SQL should return error
    migration_file2 = temp_dir / "migrations" / "0002_invalid.sql"
    migration_file2.parent.mkdir(parents=True, exist_ok=True)
    migration_file2.write_text("CRAete TABLE t (id INTEGER);\n\n" + ROLLBACK_SPLIT_TAG + "\nDROP TABLE t;")

    client.changelog.migrations.append(
        Migration(name="0002_invalid.sql", initial=False, parents=["0000_migrateit.sql"])
    )
    result = client.validate_sql_syntax(client.changelog.migrations[2])
    assert result is not None
    assert isinstance(result[0], sqlite3.Error)


def test_sqlite_cmd_init(temp_dir: Path, conn: sqlite3.Connection) -> None:
    """Test cmd_init with SQLite."""
    migrations_dir = temp_dir / "migrations"
    migrations_file = temp_dir / "changelog.json"

    cmd_init(
        table_name="migrations",
        migrations_dir=migrations_dir,
        migrations_file=migrations_file,
        database=SupportedDatabase.SQLITE,
    )

    assert migrations_dir.exists()
    assert migrations_file.exists()
    assert (migrations_dir / "0000_migrateit.sql").exists()

    content = (migrations_dir / "0000_migrateit.sql").read_text()
    assert "CREATE TABLE" in content
    assert "IF NOT EXISTS" in content
    assert ROLLBACK_SPLIT_TAG in content


def test_sqlite_cmd_run_and_rerun(temp_dir: Path, conn: sqlite3.Connection) -> None:
    """Test cmd_run with SQLite."""
    migrations_dir = temp_dir / "migrations"
    migrations_file = temp_dir / "changelog.json"

    cmd_init(
        table_name="migrations",
        migrations_dir=migrations_dir,
        migrations_file=migrations_file,
        database=SupportedDatabase.SQLITE,
    )

    # Create a migration and add to changelog
    create_migration_file(
        migrations_dir,
        "0001_create_table.sql",
        sql="CREATE TABLE test (id INTEGER PRIMARY KEY);",
    )

    changelog = load_changelog_file(migrations_file)
    from migrateit.models.migration import Migration

    changelog.migrations.append(Migration(name="0001_create_table.sql", initial=False, parents=["0000_migrateit.sql"]))
    save_changelog_file(changelog)
    config = MigrateItConfig(
        table_name="migrations",
        migrations_dir=migrations_dir,
        changelog=changelog,
    )
    client = SqliteClient(connection=conn, config=config)

    # Create the migrations table
    sql, _ = SqliteClient.create_migrations_table_str("migrations")
    conn.executescript(sql)

    # Run migration
    cmd_run(client=client)

    cursor = conn.execute("SELECT migration_name FROM migrations")
    rows = cursor.fetchall()
    assert len(rows) == 2  # migrateit + new migration

    # Re-run should be no-op
    cmd_run(client=client)
    cursor = conn.execute("SELECT migration_name FROM migrations")
    rows = cursor.fetchall()
    assert len(rows) == 2


def test_sqlite_cmd_run_fake(temp_dir: Path, conn: sqlite3.Connection) -> None:
    """Test fake migration with SQLite."""
    migrations_dir = temp_dir / "migrations"
    migrations_file = temp_dir / "changelog.json"

    cmd_init(
        table_name="migrations",
        migrations_dir=migrations_dir,
        migrations_file=migrations_file,
        database=SupportedDatabase.SQLITE,
    )

    create_migration_file(
        migrations_dir,
        "0001_fake_table.sql",
        sql="CREATE TABLE test (id INTEGER PRIMARY KEY);",
    )

    changelog = load_changelog_file(migrations_file)
    from migrateit.models.migration import Migration

    changelog.migrations.append(Migration(name="0001_fake_table.sql", initial=False, parents=["0000_migrateit.sql"]))
    save_changelog_file(changelog)

    config = MigrateItConfig(
        table_name="migrations",
        migrations_dir=migrations_dir,
        changelog=changelog,
    )
    client = SqliteClient(connection=conn, config=config)

    sql, _ = SqliteClient.create_migrations_table_str("migrations")
    conn.executescript(sql)

    # Apply the initial migration first
    cmd_run(client=client, name="0000")

    cmd_run(client=client, name="0001", is_fake=True)

    cursor = conn.execute("SELECT migration_name FROM migrations")
    rows = cursor.fetchall()
    assert len(rows) == 2  # migrateit + fake

    # Verify table was NOT created
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test'")
    assert cursor.fetchone() is None


def test_sqlite_cmd_rollback(temp_dir: Path, conn: sqlite3.Connection) -> None:
    """Test rollback with SQLite."""
    migrations_dir = temp_dir / "migrations"
    migrations_file = temp_dir / "changelog.json"

    cmd_init(
        table_name="migrations",
        migrations_dir=migrations_dir,
        migrations_file=migrations_file,
        database=SupportedDatabase.SQLITE,
    )

    create_migration_file(
        migrations_dir,
        "0001_create_table.sql",
        sql="CREATE TABLE test (id INTEGER PRIMARY KEY);",
        rollback_sql="DROP TABLE test;",
    )

    changelog = load_changelog_file(migrations_file)
    from migrateit.models.migration import Migration

    changelog.migrations.append(Migration(name="0001_create_table.sql", initial=False, parents=["0000_migrateit.sql"]))
    save_changelog_file(changelog)

    config = MigrateItConfig(
        table_name="migrations",
        migrations_dir=migrations_dir,
        changelog=changelog,
    )
    client = SqliteClient(connection=conn, config=config)

    sql, _ = SqliteClient.create_migrations_table_str("migrations")
    conn.executescript(sql)

    # Apply
    cmd_run(client=client, name="0001")

    cursor = conn.execute("SELECT migration_name FROM migrations")
    rows = cursor.fetchall()
    assert len(rows) == 2

    # Rollback
    cmd_run(client=client, name="0001", is_rollback=True)

    cursor = conn.execute("SELECT migration_name FROM migrations")
    rows = cursor.fetchall()
    assert len(rows) == 1


def test_sqlite_environment_url_default() -> None:
    """Test default SQLite environment URL."""
    with patch.dict(os.environ, {}, clear=True):
        url = SqliteClient.get_environment_url()
        assert url == "sqlite:///migrateit.db"


def test_sqlite_environment_url_from_file() -> None:
    """Test SQLite environment URL from DB_FILE."""
    with patch.dict(os.environ, {"DB_FILE": "/tmp/test.db"}):
        url = SqliteClient.get_environment_url()
        assert url == "sqlite:////tmp/test.db"


def test_sqlite_environment_url_from_db_url() -> None:
    """Test SQLite environment URL from DB_URL."""
    with patch.dict(os.environ, {"DB_URL": "sqlite:///./custom.db"}):
        url = SqliteClient.get_environment_url()
        assert url == "sqlite:///./custom.db"


def test_sqlite_validate_migrations_initial_missing(temp_dir: Path) -> None:
    """Test validation when initial migration is not defined."""
    conn = sqlite3.connect(":memory:")
    try:
        sql, _ = SqliteClient.create_migrations_table_str("migrations")
        conn.executescript(sql)
        changelog = ChangelogFile(
            version=1,
            database=SupportedDatabase.SQLITE,
            migrations=[Migration(name="0001_test.sql", initial=False, parents=[])],
            path=temp_dir / "changelog.json",
        )
        config = MigrateItConfig(
            table_name="migrations",
            migrations_dir=temp_dir / "migrations",
            changelog=changelog,
        )
        client = SqliteClient(connection=conn, config=config)
        status_map: dict[str, MigrationStatus] = {"0001_test.sql": MigrationStatus.NOT_APPLIED}
        with pytest.raises(ValueError, match="Initial migration is not defined"):
            client.validate_migrations(status_map)
    finally:
        conn.close()


def test_sqlite_safety_table_name() -> None:
    """Test unsafe table name rejection."""
    with pytest.raises(ValueError, match="Unsafe table name"):
        SqliteClient.create_migrations_table_str("invalid-name")
