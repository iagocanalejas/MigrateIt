import os
import subprocess
from collections.abc import Generator
from pathlib import Path

import pytest

from migrateit.models.config import SupportedDatabase


@pytest.fixture(autouse=True)
def migrations_dir(tmp_path_factory: pytest.TempPathFactory) -> Generator[Path]:
    temp_dir = tmp_path_factory.mktemp("migrations")
    os.environ["MIGRATEIT_MIGRATIONS_DIR"] = str(temp_dir)
    yield Path(temp_dir)


@pytest.fixture(
    params=[
        pytest.param(SupportedDatabase.POSTGRES, marks=pytest.mark.postgres, id="postgres"),
        pytest.param(SupportedDatabase.MYSQL, marks=pytest.mark.mysql, id="mysql"),
        pytest.param(SupportedDatabase.MARIADB, marks=pytest.mark.mariadb, id="mariadb"),
        pytest.param(SupportedDatabase.SQLITE, marks=pytest.mark.sqlite, id="sqlite"),
        pytest.param(SupportedDatabase.ORACLE, marks=pytest.mark.oracle, id="oracle"),
    ]
)
def database(request: pytest.FixtureRequest) -> Generator[SupportedDatabase]:
    yield request.param
    subprocess.run(["migrateit", "rollback", "--zero"], stdout=subprocess.DEVNULL)

    if request.param == SupportedDatabase.SQLITE:
        Path("migrateit.db").unlink(missing_ok=True)
