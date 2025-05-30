import contextlib
import re
import sys
from typing import IO, Any

from psycopg2 import ProgrammingError

from migrateit.models.migration import Migration, MigrationStatus

from ._utils import GREEN, NORMAL

STATUS_COLORS = {
    "reset": "\033[0m",
    MigrationStatus.APPLIED: "\033[92m",
    MigrationStatus.NOT_APPLIED: "\033[93m",
    MigrationStatus.REMOVED: "\033[94m",
    MigrationStatus.CONFLICT: "\033[91m",
}


def write(s: str, stream: IO[bytes] = sys.stdout.buffer) -> None:
    stream.write(s.encode())
    stream.flush()


def write_line_b(
    s: bytes | None = None,
    stream: IO[bytes] = sys.stdout.buffer,
    logfile_name: str | None = None,
) -> None:
    with contextlib.ExitStack() as exit_stack:
        output_streams = [stream]
        if logfile_name:
            stream = exit_stack.enter_context(open(logfile_name, "ab"))
            output_streams.append(stream)

        for output_stream in output_streams:
            if s is not None:
                output_stream.write(s)
            output_stream.write(b"\n")
            output_stream.flush()


def write_line(s: str | None = None, **kwargs: Any) -> None:
    write_line_b(s.encode() if s is not None else s, **kwargs)


def print_logo() -> None:
    write_line(GREEN)
    write_line("##########################################")
    write_line(" __  __ _                 _       ___ _")
    write_line("|  \\/  (_) __ _ _ __ __ _| |_ ___|_ _| |_")
    write_line("| |\\/| | |/ _` | '__/ _` | __/ _ \\| || __|")
    write_line("| |  | | | (_| | | | (_| | ||  __/| || |_")
    write_line("|_|  |_|_|\\__, |_|  \\__,_|\\__\\___|___|\\__|")
    write_line("          |___/")
    write_line("##########################################")
    write_line(NORMAL)


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
    write_line(f"{indent}{name:<30} | {status_str}{repeat_marker}")

    if name in seen:
        return
    seen.add(name)

    for child in children.get(name, []):
        print_dag(child.name, children, status_map, level + 1, seen)


def pretty_print_sql_error(error: ProgrammingError, sql_query: str):
    error_message = error.pgerror or str(error)

    write_line("❌ SQL Syntax Error:")
    write_line("-" * 80)
    write_line(error_message.strip())
    write_line("-" * 80)

    # Extract error position if available
    match = re.search(r"POSITION: (\d+)", error_message)
    if match:
        position = int(match.group(1))
        write_line("→ Error near here:")
        write_line(sql_query)
        write_line(" " * (position - 1) + "^")
