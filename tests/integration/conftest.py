import subprocess
from collections.abc import Generator
from pathlib import Path

import pytest

from migrateit.models.config import SupportedDatabase


@pytest.fixture(
    params=[
        pytest.param(SupportedDatabase.POSTGRES, marks=pytest.mark.postgres, id="postgres"),
        pytest.param(SupportedDatabase.MYSQL, marks=pytest.mark.mysql, id="mysql"),
        pytest.param(SupportedDatabase.MARIADB, marks=pytest.mark.mariadb, id="mariadb"),
        pytest.param(SupportedDatabase.SQLITE, marks=pytest.mark.sqlite, id="sqlite"),
        pytest.param(SupportedDatabase.ORACLE, marks=pytest.mark.oracle, id="oracle"),
    ]
)
def database(request: pytest.FixtureRequest, temp_dir: Path) -> Generator[SupportedDatabase]:
    yield request.param
    result = subprocess.run(
        ["migrateit", "rollback", "--zero"],
        cwd=temp_dir,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0

    if request.param == SupportedDatabase.SQLITE:
        Path("migrateit.db").unlink(missing_ok=True)
