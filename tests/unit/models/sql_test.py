import pytest

from migrateit.models.config import SupportedDatabase
from migrateit.models.sql import patch_sql_statement, recombine_compound_statements


@pytest.mark.unit
@pytest.mark.parametrize(
    "database, input_sql, expected_sql",
    [
        # 1. CREATE TABLE tests
        (
            SupportedDatabase.SQLITE,
            "CREATE TABLE users (id INT);",
            "CREATE TABLE IF NOT EXISTS users (id INT);",
        ),
        (
            SupportedDatabase.POSTGRES,
            "create table users (id INT);",
            "CREATE TABLE IF NOT EXISTS users (id INT);",
        ),
        (
            SupportedDatabase.POSTGRES,
            "CREATE TABLE IF NOT EXISTS users (id INT);",
            "CREATE TABLE IF NOT EXISTS users (id INT);",
        ),
        (
            SupportedDatabase.ORACLE,
            "CREATE TABLE users (id INT);",
            "CREATE TABLE users (id INT);",  # Bypassed due to early return
        ),
        # 2. DROP TABLE tests
        (
            SupportedDatabase.MYSQL,
            "DROP TABLE users;",
            "DROP TABLE IF EXISTS users;",
        ),
        (
            SupportedDatabase.ORACLE,
            "DROP TABLE users;",
            "DROP TABLE users;",  # Bypassed due to early return
        ),
        # 3. ALTER TABLE ... ADD COLUMN tests (Supported: MYSQL, MARIADB, POSTGRES)
        (
            SupportedDatabase.MYSQL,
            "ALTER TABLE users ADD COLUMN age INT;",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS age INT;",
        ),
        (
            SupportedDatabase.POSTGRES,
            "ALTER TABLE users ADD COLUMN age INT;",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS age INT;",
        ),
        (
            SupportedDatabase.MARIADB,
            "ALTER TABLE users ADD COLUMN age INT;",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS age INT;",
        ),
        (
            SupportedDatabase.SQLITE,
            "ALTER TABLE users ADD COLUMN age INT;",
            "ALTER TABLE users ADD COLUMN age INT;",  # Unsupported DB
        ),
        (
            SupportedDatabase.ORACLE,
            "ALTER TABLE users ADD COLUMN age INT;",
            "ALTER TABLE users ADD COLUMN age INT;",  # Bypassed early
        ),
        # 4. ALTER TABLE ... DROP COLUMN tests (Supported: POSTGRES only)
        (
            SupportedDatabase.POSTGRES,
            "ALTER TABLE users DROP COLUMN age;",
            "ALTER TABLE users DROP COLUMN IF EXISTS age;",
        ),
        (
            SupportedDatabase.MYSQL,
            "ALTER TABLE users DROP COLUMN age;",
            "ALTER TABLE users DROP COLUMN age;",  # Unsupported DB
        ),
        # 5. Comment stripping integration test
        (
            SupportedDatabase.SQLITE,
            "CREATE TABLE users (id INT); -- this is a comment",
            "CREATE TABLE IF NOT EXISTS users (id INT);",
        ),
    ],
)
def test_patch_sql_statement(database: SupportedDatabase, input_sql: str, expected_sql: str) -> None:
    assert patch_sql_statement(database, input_sql) == expected_sql


@pytest.mark.parametrize(
    "raw_statements, expected",
    [
        (
            ["SELECT * FROM users", "UPDATE accounts SET balance = 100", "DELETE FROM logs WHERE id = 1"],
            ["SELECT * FROM users", "UPDATE accounts SET balance = 100", "DELETE FROM logs WHERE id = 1"],
        ),
        (
            ["CREATE PROCEDURE foo() BEGIN", "UPDATE t1 SET a = 1", "UPDATE t2 SET b = 2", "END"],
            ["CREATE PROCEDURE foo() BEGIN; UPDATE t1 SET a = 1; UPDATE t2 SET b = 2; END"],
        ),
        (
            [
                "SELECT 1",
                "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW BEGIN",
                "SET NEW.col = 1",
                "END",
                "SELECT 2",
            ],
            [
                "SELECT 1",
                "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW BEGIN; SET NEW.col = 1; END",
                "SELECT 2",
            ],
        ),
        (
            ["create procedure test() begin", "select 1", "end"],
            ["create procedure test() begin; select 1; end"],
        ),
        (
            ["CREATE PROCEDURE nested() BEGIN", "BEGIN", "SELECT 1", "END", "END"],
            ["CREATE PROCEDURE nested() BEGIN; BEGIN; SELECT 1; END; END"],
        ),
        (
            ["BEGIN", "SELECT 1", "UPDATE t SET a = 2"],
            ["BEGIN; SELECT 1; UPDATE t SET a = 2"],
        ),
        (
            ["   ", "", "SELECT 1", "", "   "],
            ["SELECT 1"],
        ),
        (
            [],
            [],
        ),
    ],
    ids=[
        "normal_statements",
        "compound_block",
        "mixed_statements",
        "case_insensitive",
        "nested_blocks",
        "unclosed_compound_block",
        "whitespace_and_empty",
        "empty_list",
    ],
)
def test_recombine_compound_statements(raw_statements: list[str], expected: list[str]) -> None:
    assert recombine_compound_statements(raw_statements) == expected
