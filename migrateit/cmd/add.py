from typing import Any

import inquirer

from migrateit.clients._client import SqlClient
from migrateit.models.migration import Migration
from migrateit.models.repository import ChangelogRepository
from migrateit.reporters import write_line
from migrateit.reporters.output import write_line_error


def cmd_add(
    client: SqlClient[Any],
    name: str,
    dependencies: tuple[str, ...] | None = None,
    interactive: bool = False,
) -> int:
    if interactive and dependencies is not None and len(dependencies) > 0:
        write_line_error("Cannot specify both `--interactive` and `--dependencies`.")
        return 1

    files = list(Migration.migrations_dir.glob(f"{name.split('.')[0]}*.sql"))
    if len(files) > 1:
        write_line_error(f"Multiple migration files found for '{name}'. Please specify a unique name.")
        return 1
    if len(files) == 0:
        write_line_error(f"No migration file found for '{name}'. Please specify a valid name.")
        return 1
    name = files[0].name
    Migration.assert_valid_name(name)

    if interactive:
        try:
            choices = inquirer.prompt(
                [
                    inquirer.Checkbox(
                        "dependencies",
                        message="Select dependencies for the migration",
                        choices=[m.name for m in client.changelog.migrations],
                    )
                ],
                raise_keyboard_interrupt=True,
            )
        except KeyboardInterrupt:  # pragma: no cover
            write_line_error("Aborted by user.")
            return 1
        dependencies = choices.get("dependencies", None) if choices else None

    if not dependencies:
        write_line_error("No dependencies specified for the migration.")
        return 1
    if not all(client.migrations.exist_migration_by_name(dep) for dep in dependencies):
        write_line_error(f"Some dependencies {dependencies} do not exist in the changelog.")
        return 1
    dependencies = tuple(client.migrations.get_migration_by_name(dep).name for dep in dependencies)

    migration = Migration(name=name, initial=False, parents=dependencies)
    write_line(f"Adding migration '{migration.name}'.")

    client.changelog.migrations.append(migration)
    ChangelogRepository.save(client.changelog)
    write_line(f"Migration '{migration.name}' added successfully.")
    return 0
