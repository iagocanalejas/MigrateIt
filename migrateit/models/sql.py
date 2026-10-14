import re
from collections.abc import Generator

from sqlfluff.core import Linter

linter = Linter(dialect="ansi")
WHITESPACE_RE = re.compile(r"\s+")


def remove_sql_comments(sql: str) -> str:
    parsed = linter.parse_string(sql)
    return "".join(segment.raw for segment in parsed.tree.raw_segments if not segment.is_type("comment")).strip()


def sql_to_statements(sql: str) -> Generator[str]:
    for statement in sql.split(";"):
        stmt = statement.strip()
        if stmt:
            yield stmt
