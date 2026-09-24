import os
from pathlib import Path

import pytest

from migrateit.cli import cmd_init, cmd_new
from migrateit.clients.psql import PsqlClient
from migrateit.models.changelog import SupportedDatabase
from migrateit.models.config import MigrateItConfig
from migrateit.tree import load_changelog_file


def _setup_test_client(cmd_client: PsqlClient, temp_dir: Path) -> PsqlClient:
    cmd_init(
        table_name="migrations",
        migrations_dir=cmd_client.migrations_dir,
        migrations_file=cmd_client.changelog.path,
        database=SupportedDatabase.POSTGRES,
    )

    changelog = load_changelog_file(cmd_client.changelog.path)
    config = MigrateItConfig(
        table_name="migrations",
        migrations_dir=cmd_client.migrations_dir,
        changelog=changelog,
    )
    client = PsqlClient(connection=cmd_client.connection, config=config)
    return client


def test_cmd_new(cmd_client: PsqlClient, temp_dir: Path) -> None:
    client = _setup_test_client(cmd_client, temp_dir)
    cmd_new(client=client, name="test_migration", no_edit=True)

    assert os.path.exists(client.migrations_dir / "0001_test_migration.sql")

    changelog = load_changelog_file(client.changelog.path)
    assert len(changelog.migrations) == 2
    assert changelog.migrations[1].name == "0001_test_migration.sql"


def test_cmd_new_with_existing_migration(cmd_client: PsqlClient, temp_dir: Path) -> None:
    client = _setup_test_client(cmd_client, temp_dir)

    with open(client.migrations_dir / "0001_test_migration.sql", "w", encoding="utf-8") as f:
        f.write("Hello, world!\n")

    with pytest.raises(FileExistsError) as ctx:
        cmd_new(client=client, name="test_migration", no_edit=True)

    assert "already exists" in str(ctx.value)


def test_cmd_new_with_dependencies(cmd_client: PsqlClient, temp_dir: Path) -> None:
    client = _setup_test_client(cmd_client, temp_dir)

    cmd_new(client=client, name="test_migration", no_edit=True)
    cmd_new(client=client, name="test_migration", dependencies=["0000", "0001"], no_edit=True)

    assert os.path.exists(client.migrations_dir / "0002_test_migration.sql")

    changelog = load_changelog_file(client.changelog.path)
    assert len(changelog.migrations) == 3
    assert changelog.migrations[2].name == "0002_test_migration.sql"
    assert changelog.migrations[2].parents == ["0000_migrateit.sql", "0001_test_migration.sql"]
