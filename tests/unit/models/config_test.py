from pathlib import Path

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
    config = MigrateItConfig(
        table_name=table_name,  # type: ignore[arg-type]
        migrations_dir=Path("/tmp/migrations"),
        changelog=Changelog(version=1, path=Path("/tmp/changelog.json")),
    )
    with pytest.raises(expected_error, match=expected_match):
        MigrateItConfig.validate(config)


@pytest.mark.unit
def test_validate_config_missing_migrations_dir() -> None:
    config = MigrateItConfig(
        table_name="ok",
        migrations_dir=None,  # type: ignore[arg-type]
        changelog=Changelog(version=1, path=Path("/tmp/x.json")),
    )
    with pytest.raises(ValueError, match="Migrations directory is required"):
        MigrateItConfig.validate(config)


@pytest.mark.unit
def test_validate_config_missing_changelog_path() -> None:
    config = MigrateItConfig(
        table_name="ok",
        migrations_dir=Path("/tmp/migrations"),
        changelog=None,  # type: ignore[arg-type]
    )
    with pytest.raises(ValueError, match="Migrations file is required"):
        MigrateItConfig.validate(config)
