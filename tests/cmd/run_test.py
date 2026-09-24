from pathlib import Path

import psycopg
import pytest

from migrateit.cli import cmd_new, cmd_run
from migrateit.clients.psql import PsqlClient
from tests.conftest import create_migration_file, setup_test_client


def test_cmd_run_and_rerun(pg_conn: psycopg.Connection, cmd_client: PsqlClient, temp_dir: Path) -> None:
    client = setup_test_client(pg_conn, cmd_client)
    cmd_new(client, name="new", no_edit=True)
    create_migration_file(client.migrations_dir, "0001_new.sql", sql="SELECT 1;")

    cmd_run(client=client)
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM migrations")
        rows = cursor.fetchall()
        assert len(rows) == 2

    cmd_run(client=client)
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM migrations")
        rows = cursor.fetchall()
        assert len(rows) == 2


def test_cmd_run_by_name(pg_conn: psycopg.Connection, cmd_client: PsqlClient, temp_dir: Path) -> None:
    client = setup_test_client(pg_conn, cmd_client)
    cmd_new(client, name="new", no_edit=True)
    create_migration_file(client.migrations_dir, "0001_new.sql", sql="SELECT 1;")

    cmd_run(client=client, name="0001")
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM migrations")
        rows = cursor.fetchall()
        assert len(rows) == 2


def test_cmd_run_by_name_not_found(cmd_client: PsqlClient) -> None:
    with pytest.raises(ValueError) as ctx:
        cmd_run(client=cmd_client, name="0010")
    assert "Migration '0010' not found" in str(ctx.value)


def test_cmd_run_fake(pg_conn: psycopg.Connection, cmd_client: PsqlClient, temp_dir: Path) -> None:
    client = setup_test_client(pg_conn, cmd_client)
    cmd_new(client, name="new", no_edit=True)
    create_migration_file(client.migrations_dir, "0001_new.sql", sql="CREATE TABLE test (id serial primary key);")

    cmd_run(client=client, name="0001", is_fake=True)
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM migrations")
        rows = cursor.fetchall()
        assert len(rows) == 1

        with pytest.raises(psycopg.errors.UndefinedTable):
            cursor.execute("SELECT * FROM test")
        pg_conn.rollback()


def test_cmd_run_rollback(pg_conn: psycopg.Connection, cmd_client: PsqlClient, temp_dir: Path) -> None:
    client = setup_test_client(pg_conn, cmd_client)
    cmd_new(client, name="new", no_edit=True)
    create_migration_file(
        client.migrations_dir,
        "0001_new.sql",
        sql="CREATE TABLE test (id serial primary key);",
        rollback_sql="DROP TABLE test;",
    )

    cmd_run(client=client, name="0001")
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM migrations")
        rows = cursor.fetchall()
        assert len(rows) == 2
        cursor.execute("SELECT * FROM test")
        rows = cursor.fetchall()
        assert len(rows) == 0

    cmd_run(client=client, name="0001", is_rollback=True)
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM migrations")
        rows = cursor.fetchall()
        assert len(rows) == 1
