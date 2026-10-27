import pytest
from sqlfluff.core import Linter, SQLParseError

from migrateit.models.config import SupportedDatabase
from migrateit.models.sql import ensure_semicolon, patch_sql_statement, sql_to_statements


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
            "CREATE TABLE IF NOT EXISTS users (id INT);",  # Bypassed due to early return
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
            "DROP TABLE IF EXISTS users;",  # Bypassed due to early return
        ),
        # 3. CREATE INDEX tests
        (
            SupportedDatabase.SQLITE,
            "CREATE INDEX users_idx ON users;",
            "CREATE INDEX IF NOT EXISTS users_idx ON users;",
        ),
        # 4. DROP INDEX tests
        (
            SupportedDatabase.SQLITE,
            "DROP INDEX users_idx;",
            "DROP INDEX IF EXISTS users_idx;",
        ),
        # 5. ALTER TABLE ... ADD COLUMN tests (Supported: MYSQL, MARIADB, POSTGRES)
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
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS age INT;",  # Unsupported DB
        ),
        (
            SupportedDatabase.ORACLE,
            "ALTER TABLE users ADD age INT;",
            "ALTER TABLE users ADD age INT;",  # Bypassed early
        ),
        # 6. ALTER TABLE ... DROP COLUMN tests (Supported: POSTGRES only)
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
        # 7. Comment stripping integration test
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
    "sql_input, expected_output, errors",
    [
        (
            "SELECT 1; SELECT 2;",
            ["SELECT 1", "SELECT 2"],
            [],
        ),
        (
            "BEGIN SELECT 1; END;",
            ["BEGIN; SELECT 1; END"],
            [],
        ),
        (
            "SELECT FROM;",
            ["SELECT FROM"],
            [SQLParseError("Line 1, Position 8: Found unparsable section: 'FROM'")],
        ),
        (
            "BEGIN SELECT 1; END; SELECT 2;",
            ["BEGIN; SELECT 1; END", "SELECT 2"],
            [],
        ),
        (
            "BEGIN SELECT 1;",
            ["BEGIN; SELECT 1"],
            [],
        ),
        (
            "SELEKT * FRM non_existing_table;",
            [],
            [SQLParseError("Line 1, Position 1: Found unparsable section: 'SELEKT * FRM non_existing_table;'")],
        ),
    ],
    ids=[
        "normal_statements",
        "begin_end_block",
        "parse_error",
        "mixed_statements",
        "begin_block",
        "parse_error_non_existing_table",
    ],
)
def test_sql_to_statements(sql_input: str, expected_output: list[str], errors: list[str]) -> None:
    # Use 'postgres' dialect so BEGIN...END is parsed successfully
    linter = Linter(dialect="postgres")
    result, err = sql_to_statements(linter, sql_input)
    assert result == expected_output
    for idx, e in enumerate(errors):
        assert isinstance(e, SQLParseError)
        assert str(e) == str(err[idx])


@pytest.mark.unit
@pytest.mark.parametrize(
    "input_sql, expected_output",
    [
        ("SELECT 1;", "SELECT 1;"),
        ("SELECT 1", "SELECT 1;"),
        ("", ";"),
        ("-- This is a comment", "-- This is a comment;"),
    ],
)
def test_ensure_semicolon(input_sql: str, expected_output: str) -> None:
    assert ensure_semicolon(input_sql) == expected_output
