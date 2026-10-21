import re
from typing import Any

import sqlglot

from migrateit.models.config import SupportedDatabase

WHITESPACE_RE = re.compile(r"\s+")


def remove_sql_comments(dialect: SupportedDatabase, sql: str) -> str:
    """Removes comments from an SQL string using sqlglot."""
    if dialect == SupportedDatabase.MARIADB:
        dialect = SupportedDatabase.MYSQL
    expressions = sqlglot.parse(sql, dialect=dialect.value)
    sql = "; ".join(expr.sql(dialect=dialect.value, comments=False) for expr in expressions if expr).strip()
    return sql if sql.endswith(";") else f"{sql};"


def sql_to_statements(dialect: SupportedDatabase, sql: str) -> tuple[list[str], list[dict[str, Any]]]:
    """Splits an SQL string into individual statements and collects any parse errors/violations."""
    if dialect == SupportedDatabase.MARIADB:
        dialect = SupportedDatabase.MYSQL
    errors = []
    try:
        expressions = sqlglot.parse(sql, dialect=dialect.value, error_level=sqlglot.ErrorLevel.RAISE)
    except sqlglot.ParseError as e:
        expressions = getattr(e, "expressions", [])
        errors = e.errors
    statements = [expr.sql(dialect=dialect.value).strip() for expr in expressions if expr]
    return [s for s in statements if s], errors


def patch_sql_statement(database: SupportedDatabase, sql: str) -> str:
    sql = remove_sql_comments(database, sql)
    upper_sql = sql.upper()

    if database == SupportedDatabase.ORACLE:
        return sql

    if "CREATE TABLE" in upper_sql and "IF NOT EXISTS" not in upper_sql:
        return re.sub("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", sql, flags=re.IGNORECASE)
    if "DROP TABLE" in upper_sql and "IF EXISTS" not in upper_sql:
        return re.sub("DROP TABLE", "DROP TABLE IF EXISTS", sql, flags=re.IGNORECASE)

    if database in {SupportedDatabase.MYSQL, SupportedDatabase.MARIADB, SupportedDatabase.POSTGRES}:
        if "ALTER TABLE" in upper_sql and "ADD COLUMN" in upper_sql and "IF NOT EXISTS" not in upper_sql:
            return re.sub("ADD COLUMN", "ADD COLUMN IF NOT EXISTS", sql, flags=re.IGNORECASE)
    if database in {SupportedDatabase.POSTGRES}:
        if "ALTER TABLE" in upper_sql and "DROP COLUMN" in upper_sql and "IF EXISTS" not in upper_sql:
            return re.sub("DROP COLUMN", "DROP COLUMN IF EXISTS", sql, flags=re.IGNORECASE)

    return sql
