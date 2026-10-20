import re

from sqlfluff.core import Linter, SQLBaseError

from migrateit.models.config import SupportedDatabase

linter = Linter(dialect="ansi")
WHITESPACE_RE = re.compile(r"\s+")


def remove_sql_comments(sql: str) -> str:
    parsed = linter.parse_string(sql)
    return "".join(segment.raw for segment in parsed.tree.raw_segments if not segment.is_type("comment")).strip()


def sql_to_statements(linter: Linter, sql: str) -> tuple[list[str], list[SQLBaseError]]:
    parsed = linter.parse_string(sql)

    statements = []
    buffer = []
    for stmt in [seg.raw.strip() for seg in parsed.tree.recursive_crawl("statement", recurse_into=False)]:
        buffer.append(stmt)
        combined = " ".join(buffer)

        begin_count = len(re.findall(r"\bBEGIN\b", combined, re.IGNORECASE))
        end_count = len(re.findall(r"\bEND\b", combined, re.IGNORECASE))

        if begin_count == 0:
            # Normal single-line or non-block statement
            statements.append(stmt)
            buffer = []
        elif begin_count == end_count:
            # Compound block is fully closed
            statements.append("; ".join(buffer))
            buffer = []

    if buffer:
        statements.append("; ".join(buffer))

    return [s.strip() for s in statements if s.strip()], parsed.violations


def patch_sql_statement(database: SupportedDatabase, sql: str) -> str:
    sql = remove_sql_comments(sql)
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
