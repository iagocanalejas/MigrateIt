import importlib.metadata
import os

VERSION = importlib.metadata.version("migrateit")

MIGRATEIT_ROOT_DIR = os.getenv("MIGRATEIT_MIGRATIONS_DIR", "migrateit")
MIGRATEIT_MIGRATIONS_TABLE = os.getenv("MIGRATEIT_MIGRATIONS_TABLE", "MIGRATEIT_CHANGELOG")

MIGRATION_PATTERN = r"^\d{4}_[a-zA-Z0-9_-]+\.sql$"
ROLLBACK_SPLIT_TAG = "-- Rollback migration"
DEFAULT_TIMEOUT_SECONDS = 30

VALID_EDITORS: frozenset[str] = frozenset(
    (
        "vim",
        "vi",
        "nano",
        "emacs",
        "subl",
        "code",
        "atom",
        "zed",
        "notepad.exe",
        "notepad++",
    )
)
