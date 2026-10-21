import sqlite3
from pathlib import Path
from typing import Any

import pytest

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.models.changelog import Changelog
from migrateit.models.config import MigrateItConfig, SupportedDatabase
from migrateit.models.migration import Migration


def test_export_creates_migration_file(client: SqlClient[Any]) -> None:
    _create_database_schema(client)
    content = _export_and_read(client)

    migration_path = client.changelog.migrations[1].path
    assert migration_path.is_file()

    forward_part, rollback_part = content.split(C.ROLLBACK_SPLIT_TAG)
    forward_part = forward_part.lower()
    rollback_part = rollback_part.lower()

    # contains tables
    assert "users" in forward_part
    assert "posts" in forward_part
    assert "audit_log" in forward_part
    # contains views
    assert "user_posts" in forward_part
    assert "create view" in forward_part or "create or replace view" in forward_part
    # contains indexes
    assert "idx_posts_number2" in forward_part
    # contains triggers
    assert "audit_user_update" in forward_part
    # contains constraints
    assert "check" in forward_part and "> 0" in forward_part

    # excludes migration table
    assert C.MIGRATEIT_MIGRATIONS_TABLE.lower() not in content
    # has header
    assert "-- Migration" in content
    assert "Created on" in content

    # contains rollback
    assert "drop table " in rollback_part
    assert "drop view " in rollback_part

    if client.database != SupportedDatabase.SQLITE:
        assert "5,3" in forward_part

    if client.database == SupportedDatabase.POSTGRES:
        # contains schemas
        assert "create schema" in forward_part
        assert "drop schema if exists" in rollback_part

        # contains enums
        assert "create type" in forward_part


def test_export_invalid_parents(client: SqlClient[Any]) -> None:
    client.migrations.create_new_migration(name="migration_1", dependencies=("0000_migrateit.sql",))
    migration = client.migrations.create_new_migration(name="full_export", dependencies=("0001",))

    with pytest.raises(ValueError, match="export must depend only on the initial migration"):
        client.export_database_schema(migration)


@pytest.mark.unit
def test_export_database_schema_wrong_parents(temp_dir: Path) -> None:
    changelog = Changelog(
        version=1,
        migrations=[Migration(name="0000_init.sql", initial=True, parents=())],
    )
    config = MigrateItConfig(changelog=changelog)

    from migrateit.clients.sqlite import SqliteClient

    real_conn: sqlite3.Connection = sqlite3.connect(":memory:")
    client = SqliteClient(real_conn, config)
    try:
        migration = Migration(name="0001_export.sql", parents=("0002_other.sql",))
        with pytest.raises(ValueError, match="must depend only on the initial"):
            client.export_database_schema(migration)
    finally:
        real_conn.close()


def _export_and_read(client: SqlClient[Any], migration_name: str = "full_export") -> str:
    root = client.changelog.root
    migration = client.migrations.create_new_migration(name=migration_name, dependencies=(root.name,))
    client.export_database_schema(migration)
    return migration.path.read_text()


def _create_database_schema(client: SqlClient[Any]) -> None:
    match client.database:
        case SupportedDatabase.POSTGRES:
            with client.connection.cursor() as cursor:
                cursor.execute("CREATE SCHEMA app_schema;")
                cursor.execute("""
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
                cursor.execute("""
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL DEFAULT 'Default Title',
    body TEXT,
    number2 INTEGER NOT NULL CHECK(number2 > 0) DEFAULT 1,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
""")
                cursor.execute("CREATE INDEX idx_posts_number2 ON posts(number2)")
                cursor.execute("""
CREATE VIEW user_posts AS
SELECT users.name, posts.title
FROM users JOIN posts ON users.id = posts.user_id;
""")
                cursor.execute("""
CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    no_precission NUMERIC(5),
    precision_number NUMERIC(5,3),
    old_name VARCHAR(100),
    new_name VARCHAR(100)
);
""")
                cursor.execute("""
CREATE OR REPLACE FUNCTION audit_user_update_fn()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (OLD.id, OLD.name, NEW.name);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
""")
                cursor.execute("""
CREATE TRIGGER audit_user_update
AFTER UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION audit_user_update_fn();
""")
                # Add enum type and USER-DEFINED column type
                cursor.execute("CREATE TYPE status_enum AS ENUM ('pending', 'active', 'inactive', 'archived');")
                cursor.execute("ALTER TABLE users ADD COLUMN status status_enum;")
        case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute("""
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
                cursor.execute("""
CREATE TABLE posts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(200) NOT NULL DEFAULT 'Default Title',
    body TEXT,
    number2 INT NOT NULL DEFAULT 1 CHECK(number2 > 0),
    user_id INT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
""")
                cursor.execute("CREATE INDEX idx_posts_number2 ON posts(number2)")
                cursor.execute("""
CREATE VIEW user_posts AS
SELECT users.name, posts.title
FROM users JOIN posts ON users.id = posts.user_id;
""")
                cursor.execute("""
CREATE TABLE audit_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    precision_number DECIMAL(5,3),
    old_name VARCHAR(100),
    new_name VARCHAR(100)
);
""")
                cursor.execute("""
CREATE PROCEDURE audit_user_update_proc(
    IN p_user_id INT,
    IN p_old_name VARCHAR(255),
    IN p_new_name VARCHAR(255)
)
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (p_user_id, p_old_name, p_new_name);
END;
""")
                cursor.execute("""
CREATE TRIGGER audit_user_update
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (OLD.id, OLD.name, NEW.name);
END;
""")
        case SupportedDatabase.ORACLE:
            with client.connection.cursor() as cursor:
                cursor.execute("""
CREATE TABLE users (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    email VARCHAR2(255) UNIQUE,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP
)
""")
                cursor.execute("""
CREATE TABLE posts (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    title VARCHAR2(200) DEFAULT 'Default Title' NOT NULL,
    body CLOB,
    number2 NUMBER DEFAULT 1 NOT NULL CHECK(number2 > 0),
    user_id NUMBER NOT NULL,
    CONSTRAINT fk_posts_user FOREIGN KEY (user_id) REFERENCES users(id)
)
""")
                cursor.execute("CREATE INDEX idx_posts_number2 ON posts(number2)")
                cursor.execute("""
CREATE VIEW user_posts AS
SELECT users.name, posts.title
FROM users JOIN posts ON users.id = posts.user_id
""")
                # Large view (>4000 chars) — ensures CLOB path in _process_views (lines 386-392)
                columns = ", ".join(f"1 AS col{i}" for i in range(400))
                long_select = f"SELECT {columns} FROM DUAL"
                cursor.execute(f"CREATE VIEW large_view AS {long_select}")
                cursor.execute("""
CREATE TABLE audit_log (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    user_id NUMBER DEFAULT NULL,
    precision_number NUMBER(5,3),
    old_name VARCHAR2(100),
    new_name VARCHAR2(100)
)
""")
                cursor.execute("""
CREATE OR REPLACE TRIGGER audit_user_update
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (:OLD.id, :OLD.name, :NEW.name);
END;
""")
                cursor.execute(
                    "ALTER TABLE users ADD status VARCHAR2(50) DEFAULT 'pending'"
                    "CHECK (status IN ('pending', 'active', 'inactive', 'archived'))"
                )
                # NUMBER(precision) without scale — ensures scale=None path (line 298)
                cursor.execute("ALTER TABLE users ADD col_with_precision NUMBER(10)")
                # VARCHAR2 without DEFAULT — ensures empty default branch (line 306→309)
                cursor.execute("ALTER TABLE users ADD nullable_col VARCHAR2(50)")
        case SupportedDatabase.SQLITE:
            client.connection.executescript("""
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(200) NOT NULL DEFAULT 'Default Title',
    body TEXT,
    number2 INTEGER NOT NULL CHECK(number2 > 0) DEFAULT 1,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_posts_number2 ON posts(number2);

CREATE VIEW user_posts AS
SELECT users.name, posts.title
FROM users JOIN posts ON users.id = posts.user_id;

CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    precision_number REAL,
    old_name VARCHAR(100),
    new_name VARCHAR(100)
);

CREATE TRIGGER audit_user_update
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (OLD.id, OLD.name, NEW.name);
END;
""")
        case _:  # pragma: no cover
            raise NotImplementedError(f"Database {client.database} is not supported yet")
    _add_exclusions(client)
    client.connection.commit()


def _add_exclusions(client: SqlClient[Any]) -> None:
    match client.database:
        case SupportedDatabase.POSTGRES:
            with client.connection.cursor() as cursor:
                cursor.execute(f"""CREATE SEQUENCE {C.MIGRATEIT_MIGRATIONS_TABLE}_sequence;""")
                cursor.execute(f"CREATE INDEX idx_mig_changelog ON {client._q(C.MIGRATEIT_MIGRATIONS_TABLE)}(id);")
                cursor.execute("""
CREATE OR REPLACE FUNCTION mig_changelog_audit_fn()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (NEW.id, 'insert', 'new');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
""")
                cursor.execute(f"""
CREATE TRIGGER mig_changelog_audit
AFTER INSERT ON {client._q(C.MIGRATEIT_MIGRATIONS_TABLE)}
FOR EACH ROW
EXECUTE FUNCTION mig_changelog_audit_fn();
""")
        case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(f"CREATE INDEX idx_mig_changelog ON {client._q(C.MIGRATEIT_MIGRATIONS_TABLE)}(id);")
                cursor.execute(f"""
CREATE TRIGGER mig_changelog_audit
AFTER INSERT ON {client._q(C.MIGRATEIT_MIGRATIONS_TABLE)}
FOR EACH ROW
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (NEW.id, 'insert', 'new');
END;
""")
        case SupportedDatabase.ORACLE:
            with client.connection.cursor() as cursor:
                cursor.execute(f"""
CREATE OR REPLACE TRIGGER mig_changelog_audit
AFTER INSERT ON {client._q(C.MIGRATEIT_MIGRATIONS_TABLE)}
FOR EACH ROW
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (:NEW.id, 'insert', 'new');
END;
""")
        case SupportedDatabase.SQLITE:
            client.connection.executescript(f"""
CREATE INDEX idx_mig_changelog ON {client._q(C.MIGRATEIT_MIGRATIONS_TABLE)}(id);
CREATE TRIGGER mig_changelog_audit
AFTER INSERT ON {client._q(C.MIGRATEIT_MIGRATIONS_TABLE)}
FOR EACH ROW
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (NEW.id, 'insert', 'new');
END;
""")
        case _:  # pragma: no cover
            raise NotImplementedError(f"Database {client.database} is not supported yet")
