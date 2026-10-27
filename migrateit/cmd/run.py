from pathlib import Path
from typing import Any

import inquirer

from migrateit.clients._client import SqlClient
from migrateit.clients._lock import DatabaseLock
from migrateit.models.migration import (
    Migration,
    MigrationStatus,
)
from migrateit.reporters import write_line
from migrateit.reporters.output import write_line_error


def cmd_run(
    client: SqlClient[Any],
    name: str | None = None,
    is_rollback: bool = False,
    is_forced: bool = False,
    is_fake: bool = False,
    is_hash_update: bool = False,
    is_plan_only: bool = False,
    interactive: bool = False,
) -> int:
    if (is_fake or is_rollback or is_forced) and is_hash_update:
        if is_fake:
            write_line_error("Cannot run `--update-hash` with `--fake`.")
        if is_rollback:
            write_line_error("Cannot run `migrateit rollback` with `--update-hash`.")
        if is_forced:
            write_line_error("Cannot run `--update-hash` and `--force`.")
        return 1

    if is_hash_update:
        return _cmd_run_hash_update(client, name, interactive=interactive)

    if is_rollback and not (name or interactive):
        write_line_error("`migrateit rollback` requires a target migration name.")
        return 1

    if is_forced and interactive:
        write_line_error("`--forze` and `--interactive` cannot be used together.")
        return 1

    if is_forced and not name:
        write_line_error("`--forze` requires a target migration.")
        return 1

    try:
        migration_plan = _build_migration_plan(
            client,
            name,
            is_rollback=is_rollback,
            is_forced=is_forced,
            interactive=interactive,
        )
    except KeyboardInterrupt:  # pragma: no cover
        write_line_error("Aborted by user.")
        return 1

    if not migration_plan:
        if is_rollback:
            write_line("No migrations to roll back.")
        else:
            write_line("All migrations already applied.")
        return 0

    if is_plan_only:
        write_line(" -> ".join(m.name for m in migration_plan))
        return 0

    if is_fake:
        action = "Faking" if not is_rollback else "Faking rollback for"
    else:
        action = "Applying" if not is_rollback else "Rolling back"
    write_line(f"{action} {len(migration_plan)} migration(s).")

    with DatabaseLock(client, client.table_name):
        try:
            for migration in migration_plan:
                write_line(f"\t{action.lower().capitalize()} migration: '{migration.name}'")
                client.apply_migration(
                    migration,
                    is_rollback=is_rollback,
                    is_forced=is_forced,
                    is_fake=is_fake,
                )
            client.connection.commit()
        except Exception as e:
            try:
                client.connection.rollback()
            except Exception:  # pragma: no cover
                pass
            write_line_error(f"{e}")
            return 1

    direction = "Rollback" if is_rollback else "Migration"
    action = "rolled back" if is_rollback else "applied"
    write_line(f"{direction} complete: {len(migration_plan)} migration(s) {action}.")
    return 0


def _cmd_run_hash_update(client: SqlClient[Any], name: str | None, interactive: bool) -> int:
    if interactive:
        if name is not None:
            name = client.migrations.get_migration_by_name(name).name
        try:
            target_name = inquirer.prompt(
                [
                    inquirer.List(
                        "target",
                        message="Select migration to update hash for:",
                        choices=[m.name for m in client.changelog.migrations],
                        default=name,
                    )
                ],
                raise_keyboard_interrupt=True,
            )
        except KeyboardInterrupt:  # pragma: no cover
            write_line_error("Aborted by user.")
            return 1
        name = target_name.get("target", None) if target_name else None

    if name is None:
        write_line_error("`--update-hash` requires a target migration name.")
        return 1

    target_migration = client.migrations.get_migration_by_name(name)
    if target_migration.initial:
        write_line_error("Cannot update the hash of the initial migration.")
        return 1

    write_line(f"Updating hash for migration: '{target_migration.name}'.")
    client.update_migration_hash(target_migration)
    client.connection.commit()
    write_line(f"Hash updated for '{target_migration.name}'.")
    return 0


def _build_migration_plan(
    client: SqlClient[Any],
    name: str | None,
    is_rollback: bool = False,
    is_forced: bool = False,
    interactive: bool = False,
) -> list[Migration]:
    if is_forced:
        assert name is not None, "Forced migration requires a name."  # NOTE: checked in `cmd_run()`
        return [Migration(name=Path(name).with_suffix(".sql").name)]

    statuses = client.retrieve_migration_statuses()
    client.validate_migrations(statuses)

    target_migration = client.migrations.get_migration_by_name(name) if name else None
    if target_migration:
        write_line(f"Target: '{target_migration.name}'")

    if not interactive:
        return client.migrations.build_migration_plan(
            statuses_map=statuses,
            target_migration=target_migration,
            is_rollback=is_rollback,
        )
    return _build_interactive_plan(client, statuses, is_rollback=is_rollback)


def _build_interactive_plan(
    client: SqlClient[Any],
    statuses: dict[str, MigrationStatus],
    is_rollback: bool,
) -> list[Migration]:
    applied_set = {k for k, v in statuses.items() if v == MigrationStatus.APPLIED}
    non_applied_set = {k for k, v in statuses.items() if v == MigrationStatus.NOT_APPLIED}
    choice_list = sorted(applied_set, reverse=True) if is_rollback else sorted(non_applied_set)

    choices = inquirer.prompt(
        [
            inquirer.Checkbox(
                "migrations",
                message=f"Select migrations to {'rollback' if is_rollback else 'apply'}.",
                choices=choice_list,
            )
        ],
        raise_keyboard_interrupt=True,
    )

    selected_names = choices.get("migrations") if choices else None
    if not selected_names:
        return []

    migration_plan = [client.migrations.get_migration_by_name(m) for m in selected_names]
    if is_rollback:
        migration_plan.reverse()

    plan_name_set = {m.name for m in migration_plan}
    has_errors = False

    for m in migration_plan:
        if is_rollback:
            children = [c for c in client.changelog.migrations if m.name in c.parents]
            if any(c.name not in non_applied_set and c.name not in plan_name_set for c in children):
                write_line_error(f"Cannot rollback migration '{m.name}': its child(ren) are not rolled back.")
                has_errors = True
        else:
            if any(p not in applied_set and p not in plan_name_set for p in m.parents):
                write_line_error(f"Cannot apply migration '{m.name}': its parent(s) are not applied.")
                has_errors = True

    if has_errors:
        return []

    return migration_plan
