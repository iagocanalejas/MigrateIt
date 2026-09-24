import os
from pathlib import Path

import pytest
from psycopg import ProgrammingError

from migrateit.clients import PsqlClient
from migrateit.models import Migration
from tests.conftest import INIT_MIGRATION, TEST_MIGRATIONS_TABLE, create_migration_file


def test_validate_simple_select_syntax(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0001_init.sql"
    create_migration_file(migrations_dir, filename, sql=f"SELECT * FROM {TEST_MIGRATIONS_TABLE};")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert client.validate_sql_syntax(migration) is None


def test_validate_simple_select_with_rollback(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0001_init.sql"
    create_migration_file(
        migrations_dir,
        filename,
        sql=f"SELECT * FROM {TEST_MIGRATIONS_TABLE};",
        rollback_sql="SELECT 1;",
    )
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert client.validate_sql_syntax(migration) is None


def test_validate_create_table_syntax(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0002_create_table.sql"
    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            CREATE TABLE {TEST_MIGRATIONS_TABLE}_extra (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """,
    )
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert client.validate_sql_syntax(migration) is None
    with client.connection.cursor() as cursor:
        with pytest.raises(ProgrammingError):
            cursor.execute(f"SELECT * FROM {TEST_MIGRATIONS_TABLE}_extra;")
    client.connection.rollback()


def test_invalid_sql_in_migration_code(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0003_invalid.sql"
    create_migration_file(migrations_dir, filename, sql="SELEKT * FRM non_existing_table;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    error_result = client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, ProgrammingError)
    assert "SELEKT" in sql


def test_invalid_sql_in_rollback_code(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0004_invalid_rollback.sql"
    create_migration_file(
        migrations_dir,
        filename,
        sql=f"SELECT * FROM {TEST_MIGRATIONS_TABLE};",
        rollback_sql="ROLLBAK;",
    )
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    error_result = client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, ProgrammingError)
    assert "ROLLBAK" in sql


def test_empty_sql_file_is_skipped(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0005_empty.sql"
    create_migration_file(migrations_dir, filename, sql="")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert client.validate_sql_syntax(migration) is None


def test_file_not_found_raises_error(client: PsqlClient, temp_dir: Path) -> None:
    migration = Migration(name="not_exist.sql", parents=[INIT_MIGRATION])
    with pytest.raises(FileNotFoundError):
        client.validate_sql_syntax(migration)


def test_non_sql_file_raises_error(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0006_script.txt"
    path = migrations_dir / filename
    path.write_text("SELECT 1;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    with pytest.raises(FileNotFoundError):
        client.validate_sql_syntax(migration)


def test_validate_multiple_statements(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0007_multi.sql"
    create_migration_file(
        migrations_dir,
        filename,
        sql=f"""
            INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES ('1', 'hash1');
            INSERT INTO {TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES ('2', 'hash2');
        """,
    )
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert client.validate_sql_syntax(migration) is None


def test_validate_drop_table_statement(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0008_drop_table.sql"
    create_migration_file(migrations_dir, filename, sql=f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}_to_drop;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert client.validate_sql_syntax(migration) is None


def test_validate_alter_table_add_column(client: PsqlClient, temp_dir: Path) -> None:
    # First, create the table
    with client.connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {TEST_MIGRATIONS_TABLE}_alter (id INT);")
        client.connection.commit()

    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0009_alter_table_add.sql"
    create_migration_file(
        migrations_dir, filename, sql=f"ALTER TABLE {TEST_MIGRATIONS_TABLE}_alter ADD COLUMN new_col TEXT;"
    )
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert client.validate_sql_syntax(migration) is None

    with client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}_alter;")
        client.connection.commit()


def test_validate_alter_table_drop_column(client: PsqlClient, temp_dir: Path) -> None:
    # First, create the table with the column
    with client.connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {TEST_MIGRATIONS_TABLE}_alter2 (id INT, to_remove TEXT);")
        client.connection.commit()

    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0010_alter_table_drop.sql"
    create_migration_file(
        migrations_dir, filename, sql=f"ALTER TABLE {TEST_MIGRATIONS_TABLE}_alter2 DROP COLUMN to_remove;"
    )
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert client.validate_sql_syntax(migration) is None

    with client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}_alter2;")
        client.connection.commit()


def test_invalid_drop_table_statement(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0011_invalid_drop.sql"
    create_migration_file(migrations_dir, filename, sql="DROP TABL test_table;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    error_result = client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, ProgrammingError)
    assert "DROP TABL" in sql


def test_invalid_alter_table_statement(client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0012_invalid_alter.sql"
    create_migration_file(migrations_dir, filename, sql="ALTER TABLE some_table ADD COLUM typo_col TEXT;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    error_result = client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, ProgrammingError)
    assert "ADD COLUM" in sql
