from unittest.mock import patch

import pytest

from migrateit.models.changelog import Changelog
from migrateit.models.config import MigrateItConfig


@pytest.mark.unit
@pytest.mark.parametrize(
    "table_name,expected_error,expected_match",
    [
        pytest.param("", ValueError, "Table name is required", id="empty"),
        pytest.param(123, TypeError, None, id="non_string"),
        pytest.param("invalid-name", ValueError, "valid identifier", id="invalid_identifier"),
    ],
)
def test_validate_config(table_name: object, expected_error: type[Exception], expected_match: str | None) -> None:
    with patch("migrateit.clients._client.C.MIGRATEIT_MIGRATIONS_TABLE", table_name):
        with pytest.raises(expected_error, match=expected_match):
            MigrateItConfig(changelog=Changelog(version=1))


@pytest.mark.unit
def test_validate_config_missing_changelog_path() -> None:
    with pytest.raises(ValueError, match="Migrations file is required"):
        MigrateItConfig(
            changelog=None,  # type: ignore[arg-type]
        )
