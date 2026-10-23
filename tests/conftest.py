import functools
import importlib
import os
import tempfile
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from migrateit import constants as C
from migrateit.models.changelog import Changelog
from migrateit.models.migration import Migration


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session: pytest.Session) -> None:
    print("\n\033[1;33m⚠️  WARNING: @functools.cached_property is globally DISABLED for tests.\n\033[0m")

    # Globally patch cached_property.__get__ for the whole session
    original_get = functools.cached_property.__get__

    def bypass_cache_get[T](self: Any, instance: Callable[[Any], T] | None, owner: Any = None) -> T:  # pragma: no cover
        if instance is None:
            return self
        return self.func(instance)

    functools.cached_property.__get__ = bypass_cache_get  # type: ignore

    # Store original_get on the session so we can restore it if needed
    # (though pytest teardown isn't strictly necessary for session-end globals)
    session._original_cached_property_get = original_get  # type: ignore


@pytest.fixture()
def temp_dir() -> Generator[Path]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture(autouse=True)
def change_globals(temp_dir: Path) -> Generator[None]:
    os.environ["MIGRATEIT_MIGRATIONS_DIR"] = str(temp_dir)
    importlib.reload(C)
    assert C.MIGRATEIT_ROOT_DIR == temp_dir
    Changelog.path = C.MIGRATEIT_CHANGELOG_FILE
    Migration.migrations_dir = C.MIGRATEIT_MIGRATIONS_DIR
    yield


@pytest.fixture(autouse=True, scope="session")
def suppress_write() -> Generator[None]:
    with (
        patch("migrateit.reporters.output.write_line_b", lambda *_: None),
        patch("migrateit.reporters.output.write_line", lambda *_: None),
    ):
        yield
