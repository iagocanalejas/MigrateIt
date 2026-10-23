import importlib.metadata
import os
from pathlib import Path

VERSION = importlib.metadata.version("migrateit")

MIGRATEIT_MIGRATIONS_TABLE = os.getenv("MIGRATEIT_MIGRATIONS_TABLE", "MIGRATEIT_CHANGELOG")

MIGRATEIT_ROOT_DIR = Path(os.getenv("MIGRATEIT_MIGRATIONS_DIR", "migrateit")).resolve()
MIGRATEIT_MIGRATIONS_DIR = MIGRATEIT_ROOT_DIR / "migrations"
MIGRATEIT_CHANGELOG_FILE = MIGRATEIT_ROOT_DIR / "changelog.json"

MIGRATION_PATTERN = r"^\d{4}_[a-zA-Z0-9_-]+\.sql$"
ROLLBACK_SPLIT_TAG = "-- Rollback migration"
DEFAULT_TIMEOUT_SECONDS = "30"

VALID_EDITORS: frozenset[str] = frozenset(
    (
        "vim",
        "vi",
        "nano",
        "nvim",
        "emacs",
        "subl",
        "code",
        "atom",
        "zed",
        "notepad.exe",
        "notepad++",
    )
)
