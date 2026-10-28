import os
import platform
import shlex
import subprocess
from typing import Any

import inquirer

from migrateit import constants as C
from migrateit.clients._client import SqlClient
from migrateit.models.repository import ChangelogRepository
from migrateit.reporters import write_line
from migrateit.reporters.output import write_line_error


def _validate_editor(editor: str) -> str:
    basename = os.path.basename(editor)
    if basename not in C.VALID_EDITORS:
        if os.path.isabs(editor) and os.path.isfile(editor) and os.access(editor, os.X_OK):
            return editor
        raise ValueError(
            f"Unsafe editor: {editor!r}. "
            f"Known safe editors: {', '.join(sorted(C.VALID_EDITORS))}. "
            f"Or provide an absolute path to a valid executable."
        )
    return editor


def cmd_new(
    client: SqlClient[Any],
    name: str,
    dependencies: tuple[str, ...] | None = None,
    no_edit: bool = False,
    interactive: bool = False,
) -> int:
    if not client.is_migrations_table_created():
        write_line_error(f"Migrations table '{client.table_name}' does not exist. Please run `init` & `migrate` first.")
        return 1
    if interactive and dependencies is not None and len(dependencies) > 0:
        write_line_error("Cannot specify both `--interactive` and `--dependencies`.")
        return 1

    # NOTE: required in case-sensitive OSs
    name = name.lower()

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

    write_line(f"Creating new migration '{name}'.")
    migration = client.migrations.create_new_migration(name=name, dependencies=dependencies)
    write_line(f"Migration file created: '{migration.name}'.")
    ChangelogRepository.save(client.changelog)

    if no_edit:
        return 0

    editor = os.getenv("EDITOR", "notepad.exe" if platform.system() == "Windows" else "vim")
    editor = _validate_editor(editor)
    cmd = shlex.split(editor) + [str(migration.path)]
    try:
        rc = subprocess.run(cmd, timeout=30).returncode
    except FileNotFoundError:
        editor_name = shlex.split(editor)[0] if editor else "unknown"
        write_line_error(f"Could not find editor executable '{editor_name}'.")
        return 1
    if rc != 0:
        write_line_error(f"Editor exited with code {rc}.")
    return rc
