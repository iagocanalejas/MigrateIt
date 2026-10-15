import pytest

from migrateit.models.config import SupportedDatabase
from migrateit.models.sql import patch_sql_statement


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
