from migrateit.models import MigrationStatus

STATUS_COLORS = {
    "reset": "\033[0m",
    MigrationStatus.APPLIED: "\033[92m",
    MigrationStatus.NOT_APPLIED: "\033[93m",
    MigrationStatus.REMOVED: "\033[94m",
    MigrationStatus.CONFLICT: "\033[91m",
}


def print_dag(
    name: str,
    children: dict[str, list[str]],
    status_map: dict[str, MigrationStatus],
    level: int = 0,
    seen: set[str] | None = None,
) -> None:
    if seen is None:
        seen = set()

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
        print_dag(child, children, status_map, level + 1, seen)
