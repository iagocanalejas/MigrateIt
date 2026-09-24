import os
from pathlib import Path
from unittest.mock import patch

import pytest

from migrateit.cli import cmd_init
from migrateit.clients.psql import PsqlClient
from migrateit.models.changelog import SupportedDatabase
from migrateit.tree import ROLLBACK_SPLIT_TAG


@patch("migrateit.clients.psql.PsqlClient.create_migrations_table_str", lambda **_: ("-- create", "-- drop"))
def test_cmd_init(cmd_client: PsqlClient, temp_dir: Path) -> None:
    cmd_init(
        table_name="migrations",
        migrations_dir=cmd_client.migrations_dir,
        migrations_file=cmd_client.changelog.path,
        database=SupportedDatabase.POSTGRES,
    )

    assert os.path.exists(cmd_client.migrations_dir)
    assert os.path.exists(cmd_client.changelog.path)
    assert os.path.exists(cmd_client.migrations_dir / "0000_migrateit.sql")

    content = (cmd_client.migrations_dir / "0000_migrateit.sql").read_text()
    assert "-- create" in content
    assert "-- drop" in content
    assert ROLLBACK_SPLIT_TAG in content


def test_cmd_init_missing_rollback_tag(cmd_client: PsqlClient, temp_dir: Path) -> None:
    # Write invalid migration content before calling
    path = cmd_client.migrations_dir / "0000_migrateit.sql"
    os.makedirs(cmd_client.migrations_dir, exist_ok=True)
    path.write_text("-- Missing rollback tag")

    with pytest.raises(FileExistsError) as ctx:
        cmd_init(
            table_name="migrations",
            migrations_dir=cmd_client.migrations_dir,
            migrations_file=cmd_client.changelog.path,
            database=SupportedDatabase.POSTGRES,
        )
    assert "already exists" in str(ctx.value)
