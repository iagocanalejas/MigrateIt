from pathlib import Path

import pytest

from migrateit.cmd import cmd_init
from migrateit.constants import ROLLBACK_SPLIT_TAG
from migrateit.models.config import SupportedDatabase
from tests.unit.conftest import INITIAL_MIGRATION


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_cmd_init(database: SupportedDatabase, temp_dir: Path) -> None:
    dir_name = temp_dir / "migrations"
    cmd_init(database=database)

    assert dir_name.exists()
    assert (temp_dir / "changelog.json").exists()
    assert (dir_name / INITIAL_MIGRATION).exists()

    content = (dir_name / INITIAL_MIGRATION).read_text()
    assert "CREATE TABLE" in content
    assert ROLLBACK_SPLIT_TAG in content


@pytest.mark.unit
@pytest.mark.parametrize("database", list(SupportedDatabase), ids=lambda db: db.value)
def test_cmd_init_already_initialized(database: SupportedDatabase, temp_dir: Path) -> None:
    dir_name = temp_dir / "migrations"
    dir_name.mkdir(parents=True, exist_ok=True)
    (temp_dir / "changelog.json").touch()

    with pytest.raises(FileExistsError, match="already exists"):
        cmd_init(database=database)


@pytest.mark.unit
def test_get_init_migration_sql_unsupported() -> None:
    assert cmd_init("fake") != 0  # type: ignore[arg-type]
