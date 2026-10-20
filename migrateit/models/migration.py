from __future__ import annotations

import functools
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from migrateit import constants as C
from migrateit.models.sql import WHITESPACE_RE, remove_sql_comments


class MigrationStatus(Enum):
    APPLIED = "applied"
    CONFLICT = "conflict"
    REMOVED = "removed"
    NOT_APPLIED = "not_applied"


@dataclass(frozen=True)
class Migration:
    name: str
    initial: bool = False
    parents: tuple[str, ...] = ()

    @functools.cached_property
    def index(self) -> str:
        return self.name.split("_")[0]

    def __str__(self) -> str:  # pragma: no cover
        return self.name

    def __repr__(self) -> str:  # pragma: no cover
        return str(self)

    @staticmethod
    def is_valid_name(path: Path) -> bool:
        return path.is_file() and re.match(C.MIGRATION_PATTERN, path.name) is not None

    def get_full_path(self, migrations_dir: Path) -> Path:
        path = migrations_dir / self.name
        if not path.is_file() or not path.name.endswith(".sql"):
            raise FileNotFoundError(f"Migration file {path.name} does not exist or is not a valid SQL file")
        return path

    def get_content_and_hash(self, migrations_dir: Path) -> tuple[str, str, str]:
        content = self.get_full_path(migrations_dir).read_text()
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        parts = content.split(C.ROLLBACK_SPLIT_TAG)
        if len(parts) == 1:
            raise ValueError("No rollback tag in migration file")
        if len(parts) > 2:
            raise ValueError("Too many rollback tags in migration file")

        migration = remove_sql_comments(parts[0])
        reverse_migration = remove_sql_comments(parts[1])
        return (
            WHITESPACE_RE.sub(" ", migration).strip(),
            WHITESPACE_RE.sub(" ", reverse_migration).strip(),
            content_hash,
        )

    def rename_squashed(self, migrations_dir: Path) -> Migration:
        new_name = f"squashed_{self.name}"
        (migrations_dir / self.name).rename(migrations_dir / new_name)
        return Migration(
            name=new_name,
            initial=self.initial,
            parents=self.parents,
        )

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
        # NOTE: empty migrations can be skipped
        return

    migration_content = migration_file.read_text(encoding="utf-8")
    if C.ROLLBACK_SPLIT_TAG not in migration_content:
        raise ValueError(f"{migration_file=} does not contain a rollback section ({C.ROLLBACK_SPLIT_TAG})")

    parts = migration_content.split(C.ROLLBACK_SPLIT_TAG, maxsplit=1)

    forward_part = parts[0].rstrip()
    if sql and sql.strip():
        forward_part = f"{forward_part}\n\n{sql.strip()}"

    rollback_part = parts[1].rstrip()
    if rollback and rollback.strip():
        rollback_part = f"{rollback_part}\n\n{rollback.strip()}"

    new_content = f"{forward_part}\n\n{C.ROLLBACK_SPLIT_TAG}\n\n{rollback_part}\n"
    new_content = re.sub(r"\n{3,}", "\n\n", new_content)

    if not migration_file.exists():
        raise FileNotFoundError(f"{migration_file=} does not exist. Removed while processing.")
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
