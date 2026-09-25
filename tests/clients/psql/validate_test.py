import os
from pathlib import Path

import pytest
from psycopg import ProgrammingError

from migrateit.clients import PsqlClient
from migrateit.models import Migration
from tests.conftest import INIT_MIGRATION, TEST_MIGRATIONS_TABLE, create_migration_file


@pytest.mark.postgres
def test_validate_simple_select_syntax(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0001_init.sql"
    create_migration_file(migrations_dir, filename, sql=f"SELECT * FROM {TEST_MIGRATIONS_TABLE};")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert pg_client.validate_sql_syntax(migration) is None


@pytest.mark.postgres
def test_validate_simple_select_with_rollback(pg_client: PsqlClient, temp_dir: Path) -> None:
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
    assert pg_client.validate_sql_syntax(migration) is None


@pytest.mark.postgres
def test_validate_create_table_syntax(pg_client: PsqlClient, temp_dir: Path) -> None:
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
    assert pg_client.validate_sql_syntax(migration) is None
    with (
        pg_client.connection.cursor() as cursor,
        pytest.raises(ProgrammingError),
    ):
        cursor.execute(f"SELECT * FROM {TEST_MIGRATIONS_TABLE}_extra;")
    pg_client.connection.rollback()


@pytest.mark.postgres
def test_invalid_sql_in_migration_code(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0003_invalid.sql"
    create_migration_file(migrations_dir, filename, sql="SELEKT * FRM non_existing_table;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    error_result = pg_client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, ProgrammingError)
    assert "SELEKT" in sql


@pytest.mark.postgres
def test_invalid_sql_in_rollback_code(pg_client: PsqlClient, temp_dir: Path) -> None:
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
    error_result = pg_client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, ProgrammingError)
    assert "ROLLBAK" in sql


@pytest.mark.postgres
def test_empty_sql_file_is_skipped(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0005_empty.sql"
    create_migration_file(migrations_dir, filename, sql="")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert pg_client.validate_sql_syntax(migration) is None


@pytest.mark.postgres
def test_file_not_found_raises_error(pg_client: PsqlClient, temp_dir: Path) -> None:
    migration = Migration(name="not_exist.sql", parents=[INIT_MIGRATION])
    with pytest.raises(FileNotFoundError):
        pg_client.validate_sql_syntax(migration)


@pytest.mark.postgres
def test_non_sql_file_raises_error(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0006_script.txt"
    path = migrations_dir / filename
    path.write_text("SELECT 1;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    with pytest.raises(FileNotFoundError):
        pg_client.validate_sql_syntax(migration)


@pytest.mark.postgres
def test_validate_multiple_statements(pg_client: PsqlClient, temp_dir: Path) -> None:
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
    assert pg_client.validate_sql_syntax(migration) is None


@pytest.mark.postgres
def test_validate_drop_table_statement(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0008_drop_table.sql"
    create_migration_file(migrations_dir, filename, sql=f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}_to_drop;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert pg_client.validate_sql_syntax(migration) is None


@pytest.mark.postgres
def test_validate_alter_table_add_column(pg_client: PsqlClient, temp_dir: Path) -> None:
    # First, create the table
    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {TEST_MIGRATIONS_TABLE}_alter (id INT);")
        pg_client.connection.commit()

    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0009_alter_table_add.sql"
    create_migration_file(
        migrations_dir, filename, sql=f"ALTER TABLE {TEST_MIGRATIONS_TABLE}_alter ADD COLUMN new_col TEXT;"
    )
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert pg_client.validate_sql_syntax(migration) is None

    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}_alter;")
        pg_client.connection.commit()


@pytest.mark.postgres
def test_validate_alter_table_drop_column(pg_client: PsqlClient, temp_dir: Path) -> None:
    # First, create the table with the column
    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {TEST_MIGRATIONS_TABLE}_alter2 (id INT, to_remove TEXT);")
        pg_client.connection.commit()

    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0010_alter_table_drop.sql"
    create_migration_file(
        migrations_dir, filename, sql=f"ALTER TABLE {TEST_MIGRATIONS_TABLE}_alter2 DROP COLUMN to_remove;"
    )
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    assert pg_client.validate_sql_syntax(migration) is None

    with pg_client.connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_MIGRATIONS_TABLE}_alter2;")
        pg_client.connection.commit()


@pytest.mark.postgres
def test_invalid_drop_table_statement(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0011_invalid_drop.sql"
    create_migration_file(migrations_dir, filename, sql="DROP TABL test_table;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    error_result = pg_client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, ProgrammingError)
    assert "DROP TABL" in sql


@pytest.mark.postgres
def test_invalid_alter_table_statement(pg_client: PsqlClient, temp_dir: Path) -> None:
    migrations_dir = temp_dir / "migrations"
    os.makedirs(migrations_dir, exist_ok=True)

    filename = "0012_invalid_alter.sql"
    create_migration_file(migrations_dir, filename, sql="ALTER TABLE some_table ADD COLUM typo_col TEXT;")
    migration = Migration(name=filename, parents=[INIT_MIGRATION])
    error_result = pg_client.validate_sql_syntax(migration)
    assert isinstance(error_result, tuple)
    error, sql = error_result
    assert isinstance(error, ProgrammingError)
    assert "ADD COLUM" in sql
