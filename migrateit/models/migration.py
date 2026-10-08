import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from migrateit import constants as C


class MigrationStatus(Enum):
    APPLIED = "applied"
    CONFLICT = "conflict"
    REMOVED = "removed"
    NOT_APPLIED = "not_applied"


@dataclass(frozen=True, slots=True)
class Migration:
    name: str
    initial: bool = False
    parents: tuple[str, ...] = ()

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return str(self)

    @staticmethod
    def is_valid_name(path: Path) -> bool:
        return path.is_file() and re.match(C.MIGRATION_PATTERN, path.name) is not None

    @staticmethod
    def is_same_migration_name(name1: str, name2: str) -> bool:
        if not name1 or not name2:
            return False
        return name1 == name2 or name1.startswith(name2.split("_")[0])

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "initial": self.initial,
            "parents": self.parents,
        }


def create_migration_directory(migrations_dir: Path) -> None:
    """
    Create the migrations directory if it doesn't exist.
    Args:
        migrations_dir: The path to the migrations directory.
    """
    migrations_dir.mkdir(parents=True, exist_ok=True)


def get_migration_header(full_path: Path) -> str:
    return f"-- Migration {full_path.name}\n-- Created on {datetime.now().isoformat()}\n\n\n"


def write_into_migration_file(migration_file: Path, sql: str | None, rollback: str | None) -> None:
    """
    Write SQL and rollback SQL into a migration file.
    Args:
        migration_file: The path to the migration file.
        sql: The SQL to write into the migration file.
        rollback: The rollback SQL to write into the migration file.
    """
    if (sql is None or not sql.strip()) and (rollback is None or not rollback.strip()):
        raise ValueError("At least one of sql or rollback must be provided")

    migration_content = migration_file.read_text(encoding="utf-8")
    if C.ROLLBACK_SPLIT_TAG not in migration_content:
        raise ValueError(f"{migration_file=} does not contain a rollback section ({C.ROLLBACK_SPLIT_TAG})")

    parts = migration_content.split(C.ROLLBACK_SPLIT_TAG, maxsplit=1)
    new_content = (
        parts[0].rstrip()
        + "\n\n"
        + (sql.strip() if sql else "")
        + "\n\n"
        + C.ROLLBACK_SPLIT_TAG
        + parts[1].rstrip()
        + "\n\n"
        + (rollback.strip() if rollback else "")
    )
    new_content = re.sub(r"\n{3,}", "\n\n", new_content)

    migration_file.write_text(new_content, encoding="utf-8")


def retrieve_migration_sqls(migration_file: Path) -> tuple[str | None, str | None]:
    """
    Retrieve the SQL and rollback SQL from a migration file.
    Args:
        migration_file: The path to the migration file.
    Returns:
        A tuple of (SQL, rollback SQL).
    """

    def remove_description_comments(content: str) -> str:
        return "\n".join(
            [
                w
                for w in content.splitlines()
                if not w.strip().startswith("-- Migration") and not w.strip().startswith("-- Created on")
            ]
        )

    if not migration_file.is_file() or not migration_file.name.endswith(".sql"):
        raise ValueError(f"Migration {migration_file.name} is not a valid SQL file")

    content = migration_file.read_text(encoding="utf-8")
    if C.ROLLBACK_SPLIT_TAG not in content:
        return remove_description_comments(content).strip(), None

    sql, rollback_sql = content.split(C.ROLLBACK_SPLIT_TAG, maxsplit=1)
    return remove_description_comments(sql).strip(), rollback_sql.strip()
