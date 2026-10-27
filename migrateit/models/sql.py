import re

from sqlfluff.core import Linter, SQLBaseError

from migrateit.models.config import SupportedDatabase

linter = Linter(dialect="ansi")
WHITESPACE_RE = re.compile(r"\s+")


def remove_sql_comments(sql: str) -> str:
    parsed = linter.parse_string(sql)
    return "".join(segment.raw for segment in parsed.tree.raw_segments if not segment.is_type("comment")).strip()


def ensure_semicolon(sql: str) -> str:
    if not sql.endswith(";"):
        sql += ";"
    return sql


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

    # 1. CREATE TABLE IF NOT EXISTS
    if re.search(r"\bCREATE\s+TABLE\b", upper_sql) and not re.search(r"\bIF\s+NOT\s+EXISTS\b", upper_sql):
        sql = re.sub(
            r"\bCREATE\s+TABLE\b",
            "CREATE TABLE IF NOT EXISTS",
            sql,
            flags=re.IGNORECASE,
            count=1,
        )
        upper_sql = sql.upper()

    # 2. DROP TABLE IF EXISTS
    if re.search(r"\bDROP\s+TABLE\b", upper_sql) and not re.search(r"\bIF\s+EXISTS\b", upper_sql):
        sql = re.sub(
            r"\bDROP\s+TABLE\b",
            "DROP TABLE IF EXISTS",
            sql,
            flags=re.IGNORECASE,
            count=1,
        )
        upper_sql = sql.upper()

    # 3. CREATE INDEX IF NOT EXISTS (Supports UNIQUE indexes too)
    if re.search(r"\bCREATE\s+(?:UNIQUE\s+)?INDEX\b", upper_sql) and not re.search(r"\bIF\s+NOT\s+EXISTS\b", upper_sql):
        sql = re.sub(
            r"\bCREATE\s+(UNIQUE\s+)?INDEX\b",
            r"CREATE \1INDEX IF NOT EXISTS",
            sql,
            flags=re.IGNORECASE,
            count=1,
        )
        upper_sql = sql.upper()

    # 4. DROP INDEX IF EXISTS
    if re.search(r"\bDROP\s+INDEX\b", upper_sql) and not re.search(r"\bIF\s+EXISTS\b", upper_sql):
        sql = re.sub(
            r"\bDROP\s+INDEX\b",
            "DROP INDEX IF EXISTS",
            sql,
            flags=re.IGNORECASE,
            count=1,
        )
        upper_sql = sql.upper()

    # 5. ALTER TABLE ... ADD [COLUMN] IF NOT EXISTS
    if (
        re.search(r"\bALTER\s+TABLE\b", upper_sql)
        and re.search(r"\bADD\s+(?:COLUMN\s+)?", upper_sql)
        and not re.search(r"\bIF\s+NOT\s+EXISTS\b", upper_sql)
    ):
        sql = re.sub(
            r"\bADD\s+COLUMN\b",
            "ADD COLUMN IF NOT EXISTS",
            sql,
            flags=re.IGNORECASE,
            count=1,
        )
        upper_sql = sql.upper()

    # 6. ALTER TABLE ... DROP COLUMN IF EXISTS
    if database in {SupportedDatabase.POSTGRES, SupportedDatabase.SQLITE}:
        if (
            re.search(r"\bALTER\s+TABLE\b", upper_sql)
            and re.search(r"\bDROP\s+COLUMN\b", upper_sql)
            and not re.search(r"\bIF\s+EXISTS\b", upper_sql)
        ):
            sql = re.sub(
                r"\bDROP\s+COLUMN\b",
                "DROP COLUMN IF EXISTS",
                sql,
                flags=re.IGNORECASE,
                count=1,
            )

    return sql
