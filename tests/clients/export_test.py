from typing import Any

import pytest

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.constants import MIGRATEIT_MIGRATIONS_TABLE
from migrateit.models.changelog import SupportedDatabase


def test_export_creates_migration_file(client: SqlClient[Any]) -> None:
    _create_database_schema(client)
    content = _export_and_read(client)

    migration_path = client.migrations_dir / client.changelog.migrations[1].name
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
    assert "idx_posts_user_id" in forward_part
    # contains triggers
    assert "audit_user_update" in forward_part
    # contains constraints
    assert "check" in forward_part and "> 0" in forward_part

    # excludes migration table
    assert MIGRATEIT_MIGRATIONS_TABLE not in content
    # has header
    assert "-- Migration" in content
    assert "Created on" in content

    # contains rollback
    assert "drop table if exists" in rollback_part
    assert "drop view if exists" in rollback_part

    if client.changelog.database == SupportedDatabase.POSTGRES:
        # contains schemas
        assert "create schema" in forward_part
        assert "drop schema if exists" in rollback_part

        # contains enums
        assert "create type" in forward_part


def test_export_invalid_parents(client: SqlClient[Any]) -> None:
    migration = client.changelog.create_new_migration(
        migrations_dir=client.migrations_dir,
        name="full_export",
        dependencies=["0000_migrateit.sql"],
    )
    migration.parents = ["some_other_migration.sql"]

    with pytest.raises(ValueError, match="export must depend only on the initial migration"):
        client.export_database_schema(migration)


def _export_and_read(client: SqlClient[Any], migration_name: str = "full_export") -> str:
    root = client.changelog.root
    migration = client.changelog.create_new_migration(
        migrations_dir=client.migrations_dir,
        name=migration_name,
        dependencies=[root.name],
    )
    client.export_database_schema(migration)
    return (client.migrations_dir / migration.name).read_text()


def _create_database_schema(client: SqlClient[Any]) -> None:
    match client.changelog.database:
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
    number INTEGER NOT NULL CHECK(number > 0) DEFAULT 1,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
""")
                cursor.execute("CREATE INDEX idx_posts_user_id ON posts(user_id);")
                cursor.execute("""
CREATE VIEW user_posts AS
SELECT users.name, posts.title
FROM users JOIN posts ON users.id = posts.user_id;
""")
                cursor.execute("""
CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
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
    number INT NOT NULL DEFAULT 1 CHECK(number > 0),
    user_id INT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
""")
                cursor.execute("CREATE INDEX idx_posts_user_id ON posts(user_id);")
                cursor.execute("""
CREATE VIEW user_posts AS
SELECT users.name, posts.title
FROM users JOIN posts ON users.id = posts.user_id;
""")
                cursor.execute("""
CREATE TABLE audit_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
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
    number INTEGER NOT NULL CHECK(number > 0) DEFAULT 1,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_posts_user_id ON posts(user_id);

CREATE VIEW user_posts AS
SELECT users.name, posts.title
FROM users JOIN posts ON users.id = posts.user_id;

CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
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
        case _:
            raise NotImplementedError(f"Database {client.changelog.database} is not supported yet")
    _add_exclusions(client)
    client.connection.commit()


def _add_exclusions(client: SqlClient[Any]) -> None:
    match client.changelog.database:
        case SupportedDatabase.POSTGRES:
            with client.connection.cursor() as cursor:
                cursor.execute(f"""CREATE TABLE {MIGRATEIT_MIGRATIONS_TABLE} (id INT PRIMARY KEY);""")
                cursor.execute(f"CREATE INDEX idx_mig_changelog ON {MIGRATEIT_MIGRATIONS_TABLE}(id);")
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
AFTER INSERT ON {MIGRATEIT_MIGRATIONS_TABLE}
FOR EACH ROW
EXECUTE FUNCTION mig_changelog_audit_fn();
""")
        case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
            with client.connection.cursor() as cursor:
                cursor.execute(f"CREATE TABLE {MIGRATEIT_MIGRATIONS_TABLE} (id INT PRIMARY KEY);")
                cursor.execute(f"CREATE INDEX idx_mig_changelog ON {MIGRATEIT_MIGRATIONS_TABLE}(id);")
                cursor.execute(f"""
CREATE TRIGGER mig_changelog_audit
AFTER INSERT ON {MIGRATEIT_MIGRATIONS_TABLE}
FOR EACH ROW
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (NEW.id, 'insert', 'new');
END;
""")
        case SupportedDatabase.SQLITE:
            client.connection.executescript(f"""
CREATE TABLE {MIGRATEIT_MIGRATIONS_TABLE} (id INTEGER PRIMARY KEY);
CREATE INDEX idx_mig_changelog ON {MIGRATEIT_MIGRATIONS_TABLE}(id);
CREATE TRIGGER mig_changelog_audit
AFTER INSERT ON {MIGRATEIT_MIGRATIONS_TABLE}
FOR EACH ROW
BEGIN
    INSERT INTO audit_log(user_id, old_name, new_name)
    VALUES (NEW.id, 'insert', 'new');
END;
""")
        case _:
            raise NotImplementedError(f"Database {client.changelog.database} is not supported yet")
