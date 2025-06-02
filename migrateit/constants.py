import importlib.metadata
import os

VERSION = importlib.metadata.version("migrateit")

MIGRATEIT_ROOT_DIR = os.getenv("MIGRATEIT_MIGRATIONS_DIR", "migrateit")
MIGRATEIT_MIGRATIONS_TABLE = os.getenv("MIGRATEIT_MIGRATIONS_TABLE", "MIGRATEIT_CHANGELOG")
