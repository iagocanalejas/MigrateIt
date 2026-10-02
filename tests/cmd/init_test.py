from pathlib import Path

import pytest

from migrateit.cmd import cmd_init
from migrateit.constants import ROLLBACK_SPLIT_TAG
from migrateit.models.changelog import SupportedDatabase
from tests.conftest import (
    INITIAL_MIGRATION,
    TEST_MIGRATIONS_TABLE,
)


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_cmd_init(database: SupportedDatabase, temp_dir: Path) -> None:
    dir_name = temp_dir / "test_dir"
    cmd_init(
        table_name=TEST_MIGRATIONS_TABLE,
        migrations_dir=dir_name,
        migrations_file=dir_name / "changelog.json",
        database=database,
    )

    assert dir_name.exists()
    assert (dir_name / "changelog.json").exists()
    assert (dir_name / INITIAL_MIGRATION).exists()

    content = (dir_name / INITIAL_MIGRATION).read_text()
    assert "CREATE TABLE" in content
    assert ROLLBACK_SPLIT_TAG in content


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_cmd_init_already_initialized(database: SupportedDatabase, temp_dir: Path) -> None:
    """Test cmd_init raises FileExistsError when changelog file already exists."""
    dir_name = temp_dir / "test_dir"
    dir_name.mkdir(parents=True, exist_ok=True)
    (dir_name / "changelog.json").touch()

    with pytest.raises(FileExistsError, match="already exists"):
        cmd_init(
            table_name=TEST_MIGRATIONS_TABLE,
            migrations_dir=dir_name,
            migrations_file=dir_name / "changelog.json",
            database=database,
        )
