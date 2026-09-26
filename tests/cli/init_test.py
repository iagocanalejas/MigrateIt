import os
import shutil
from pathlib import Path
from typing import Any

import pytest

from migrateit.cli import cmd_init
from migrateit.clients import SqlClient
from migrateit.models.changelog import SupportedDatabase
from migrateit.tree import ROLLBACK_SPLIT_TAG


@pytest.mark.integration
def test_cmd_init(cmd_client: SqlClient[Any], temp_dir: Path) -> None:
    """Test cmd_init creates directory, changelog, and migration file."""
    migrations_dir = cmd_client.migrations_dir
    migrations_file = cmd_client.changelog.path

    assert migrations_dir.exists()
    assert migrations_file.exists()
    assert (migrations_dir / "0000_migrateit.sql").exists()

    content = (migrations_dir / "0000_migrateit.sql").read_text()
    assert "CREATE TABLE" in content
    assert ROLLBACK_SPLIT_TAG in content


@pytest.mark.integration
def test_cmd_init_missing_rollback_tag(temp_dir: Path) -> None:
    """Test cmd_init raises FileExistsError when migration file already exists."""

    migrations_dir = temp_dir / "migrations"
    migrations_file = temp_dir / "changelog.json"

    # First init creates the files
    cmd_init(
        table_name="migrations",
        migrations_dir=migrations_dir,
        migrations_file=migrations_file,
        database=SupportedDatabase.SQLITE,
    )

    # Clean up the files
    shutil.rmtree(migrations_dir)
    migrations_file.unlink(missing_ok=True)

    # Pre-create the migration file with invalid content
    os.makedirs(migrations_dir, exist_ok=True)
    path = migrations_dir / "0000_migrateit.sql"
    path.write_text("-- Missing rollback tag")

    with pytest.raises(FileExistsError) as ctx:
        cmd_init(
            table_name="migrations",
            migrations_dir=migrations_dir,
            migrations_file=migrations_file,
            database=SupportedDatabase.SQLITE,
        )
    assert "already exists" in str(ctx.value)
