import re

from psycopg2 import ProgrammingError

from migrateit.models import MigrationStatus
from migrateit.models.migration import Migration

STATUS_COLORS = {
    "reset": "\033[0m",
    MigrationStatus.APPLIED: "\033[92m",
    MigrationStatus.NOT_APPLIED: "\033[93m",
    MigrationStatus.REMOVED: "\033[94m",
    MigrationStatus.CONFLICT: "\033[91m",
}


def print_dag(
    name: str,
    children: dict[str, list[Migration]],
    status_map: dict[str, MigrationStatus],
    level: int = 0,
    seen: set[str] = set(),
) -> None:
    indent = "  " * level + ("└─ " if level > 0 else "")
    status = status_map[name]
    status_str = f"{STATUS_COLORS[status]}{status.name.replace('_', ' ').title()}{STATUS_COLORS['reset']}"

    # indicate repeated visit
    repeat_marker = " (*)" if name in seen else ""
    print(f"{indent}{name:<30} | {status_str}{repeat_marker}")

    if name in seen:
        return
    seen.add(name)

    for child in children.get(name, []):
        print_dag(child.name, children, status_map, level + 1, seen)


def pretty_print_sql_error(error: ProgrammingError, sql_query: str):
    error_message = error.pgerror or str(error)

    print("❌ SQL Syntax Error:")
    print("-" * 80)
    print(error_message.strip())
    print("-" * 80)

    # Extract error position if available
    match = re.search(r"POSITION: (\d+)", error_message)
    if match:
        position = int(match.group(1))
        print("→ Error near here:")
        print(sql_query)
        print(" " * (position - 1) + "^")
