import re

from sqlfluff.core import Linter

linter = Linter(dialect="ansi")
WHITESPACE_RE = re.compile(r"\s+")


def remove_sql_comments(sql: str) -> str:
    parsed = linter.parse_string(sql)
    return "".join(segment.raw for segment in parsed.tree.raw_segments if not segment.is_type("comment")).strip()
