from __future__ import annotations

from collections import Counter
from typing import Any

from migrateit.clients._client import SqlClient
from migrateit.models.migration import (
    MigrationStatus,
)
from migrateit.reporters import STATUS_COLORS, pretty_print_sql_error, write_line


def cmd_show(client: SqlClient[Any], list_mode: bool = False, validate_sql: bool = False) -> int:
    status_map = client.retrieve_migration_statuses()
    status_count = Counter({status: 0 for status in MigrationStatus})
    status_count.update(status_map.values())

    write_line("\nMigration Precedence DAG:\n")
    write_line(f"{'Migration File':<40} | {'Status'}")
    write_line("-" * 60)

    if list_mode:
        client.migrations.print_list(status_map)
    else:
        client.migrations.print_dag(status_map=status_map)

    write_line("\nSummary:")
    for status, label in {
        MigrationStatus.APPLIED: "Applied",
        MigrationStatus.NOT_APPLIED: "Not Applied",
        MigrationStatus.REMOVED: "Removed",
        MigrationStatus.CONFLICT: "Conflict",
    }.items():
        write_line(f"  {label:<12}: {STATUS_COLORS[status]}{status_count[status]}{STATUS_COLORS['reset']}")

    not_applied = status_count[MigrationStatus.NOT_APPLIED]
    if not_applied > 0:
        write_line(f"\n→ {not_applied} migration(s) pending. Run `migrateit migrate` to apply them.")
    removed_count = status_count[MigrationStatus.REMOVED]
    if removed_count > 0:
        write_line(f"\n⚠ {removed_count} migration(s) found in database but missing from changelog.")
    conflict_count = status_count[MigrationStatus.CONFLICT]
    if conflict_count > 0:
        write_line(f"\n⚠ {conflict_count} migration(s) have hash conflicts between file and database.")

    if validate_sql:
        write_line("\nValidating SQL migrations...")
        has_err = False
        for migration in client.changelog.migrations:
            if errors := client.validate_sql_syntax(migration):
                has_err = True
                for err in errors:
                    pretty_print_sql_error(err[0], err[1])
        msg = "failed. Please fix the errors above." if has_err else "passed. No errors found."
        write_line("SQL validation " + msg)
    return 0
