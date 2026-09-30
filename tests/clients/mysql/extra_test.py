import os
from pathlib import Path

import pytest

from migrateit.clients.mysql import MySqlClient
from migrateit.models import Migration
from migrateit.models.changelog import ChangelogFile
from migrateit.models.migration import MigrationStatus
from tests.conftest import INIT_MIGRATION, TEST_MIGRATIONS_TABLE, create_migration_file

# --- create_migrations_table tests ---


@pytest.mark.mysql
def test_create_migrations_table() -> None:
    """Test that the migrations table SQL is generated correctly."""
    table_name = "test_migrations"
    sql, rollback = MySqlClient.create_migrations_table_str(table_name)
    assert "`test_migrations`" in sql
    assert "id INT AUTO_INCREMENT PRIMARY KEY" in sql
    assert "migration_name VARCHAR(255) UNIQUE NOT NULL" in sql
    assert "change_hash VARCHAR(64) NOT NULL" in sql
    assert "squashed TINYINT(1) DEFAULT 0" in sql
    assert rollback is not None


@pytest.mark.mysql
def test_safety_table_name() -> None:
    """Test unsafe table name rejection."""
    with pytest.raises(ValueError, match="Unsafe table name"):
        MySqlClient.create_migrations_table_str("invalid-name")


# --- is_migrations_table_created tests ---


@pytest.mark.mysql
def test_table_exists_returns_true(mysql_client: MySqlClient, temp_dir: Path) -> None:
    # drop the table first as is being created in pytest fixtures
    with mysql_client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
    mysql_client.connection.commit()

    sql, _ = MySqlClient.create_migrations_table_str(TEST_MIGRATIONS_TABLE)
    with mysql_client.connection.cursor() as cursor:
        cursor.execute(sql)  # pyright: ignore
        mysql_client.connection.commit()
    assert mysql_client.is_migrations_table_created()


@pytest.mark.mysql
def test_table_missing_returns_false(mysql_client: MySqlClient, temp_dir: Path) -> None:
    with mysql_client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
    mysql_client.connection.commit()
    assert not mysql_client.is_migrations_table_created()


# --- is_migration_applied tests ---


@pytest.mark.mysql
def test_applied_migration_returns_true(mysql_client: MySqlClient, temp_dir: Path) -> None:
    migration = Migration(name="0000_init.sql", initial=True, parents=[])
    migrations_dir = temp_dir / "migrations"
    create_migration_file(migrations_dir, "0000_init.sql")
    mysql_client.config.changelog = ChangelogFile(version=1, migrations=[migration])

    mysql_client.apply_migration(migration, is_fake=False)
    assert mysql_client.is_migration_applied(migration)


@pytest.mark.mysql
def test_not_applied_migration_returns_false(mysql_client: MySqlClient, temp_dir: Path) -> None:
    migration = Migration(name="0001_test.sql", parents=[INIT_MIGRATION])
    mysql_client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), migration])
    assert not mysql_client.is_migration_applied(migration)


# --- retrieve_migration_statuses tests ---


@pytest.mark.mysql
def test_no_table_returns_not_applied(mysql_client: MySqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    with mysql_client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}")
    mysql_client.connection.commit()

    create_migration_file(migrations_dir, "0001_test.sql")
    migrations = [Migration(name="0001_test.sql", parents=[INIT_MIGRATION])]
    mysql_client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), *migrations])
    statuses = mysql_client.retrieve_migration_statuses()
    assert statuses["0001_test.sql"] == MigrationStatus.NOT_APPLIED


# --- update_migration_hash tests ---


@pytest.mark.mysql
def test_update_migration_hash(mysql_client: MySqlClient, temp_dir: Path) -> None:
    filename = "0001_update_hash.sql"
    migrations_dir = temp_dir / "migrations"
    create_migration_file(migrations_dir, filename)
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    mysql_client.config.changelog = ChangelogFile(version=1, migrations=[Migration(name=INIT_MIGRATION), migration])

    mysql_client.apply_migration(migration, is_fake=False)

    # Get the current hash from DB
    with mysql_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT change_hash FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        row = cursor.fetchone()
        assert row is not None
        old_hash = row[0]  # type: ignore

    # Update the hash
    mysql_client.update_migration_hash(migration)
    mysql_client.connection.commit()

    # Get the new hash
    with mysql_client.connection.cursor() as cursor:
        cursor.execute(f"SELECT change_hash FROM {TEST_MIGRATIONS_TABLE} WHERE migration_name = %s", (filename,))
        row = cursor.fetchone()
        assert row is not None
        new_hash = row[0]  # type: ignore

    assert old_hash == new_hash  # same file, same hash


# --- environment_url tests ---


@pytest.mark.mysql
def test_environment_url_default() -> None:
    """Test default MySQL environment URL."""
    with pytest.MonkeyPatch.context() as mp:
        mp.delenv("DB_URL", raising=False)
        mp.delenv("DB_HOST", raising=False)
        mp.delenv("DB_PORT", raising=False)
        mp.delenv("DB_USER", raising=False)
        mp.delenv("DB_PASS", raising=False)
        mp.delenv("DB_NAME", raising=False)
        url = MySqlClient.get_environment_url()
        assert url == "mysql://root@localhost:3306/migrateit?connect_timeout=30"


@pytest.mark.mysql
def test_environment_url_from_env_vars() -> None:
    """Test MySQL environment URL from individual env vars."""
    with pytest.MonkeyPatch.context() as mp:
        mp.delenv("DB_URL", raising=False)
        mp.setenv("DB_HOST", "myhost")
        mp.setenv("DB_PORT", "3307")
        mp.setenv("DB_USER", "myuser")
        mp.setenv("DB_PASS", "mypassword")
        mp.setenv("DB_NAME", "mydb")
        url = MySqlClient.get_environment_url()
        assert url == "mysql://myuser:mypassword@myhost:3307/mydb?connect_timeout=30"


@pytest.mark.mysql
def test_environment_url_from_db_url() -> None:
    """Test MySQL environment URL from DB_URL."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("DB_URL", "mysql://admin@remote:3307/mydb")
        url = MySqlClient.get_environment_url()
        assert url == "mysql://admin@remote:3307/mydb"


# --- _get_database_hash tests ---


@pytest.mark.mysql
def test_get_database_hash_found(mysql_client: MySqlClient) -> None:
    """Test retrieving a hash that exists."""
    test_hash = "abc123def456"
    with mysql_client.connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (%s, %s)",
            ("0001_test.sql", test_hash),
        )
    mysql_client.connection.commit()
    hash_result = mysql_client._get_database_hash("0001_test.sql")
    assert hash_result == test_hash


@pytest.mark.mysql
def test_get_database_hash_not_found(mysql_client: MySqlClient) -> None:
    with pytest.raises(ValueError, match="not found in the database"):
        mysql_client._get_database_hash("nonexistent.sql")
