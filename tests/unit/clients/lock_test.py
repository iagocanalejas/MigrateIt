from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from migrateit.clients._client import SqlClient, get_client
from migrateit.clients._lock import DatabaseLock, _DBTableLocker, _MySqlLocker, _PsqlLocker
from migrateit.models.changelog import Changelog
from migrateit.models.config import MigrateItConfig, SupportedDatabase
from migrateit.models.migration import Migration
from tests.unit.conftest import INITIAL_MIGRATION, TEST_MIGRATIONS_TABLE, connection


@pytest.fixture(autouse=True)
def _patch_lock_timeouts() -> Generator[None]:
    with (
        patch("migrateit.clients._lock.LOCK_TIMEOUT_SECONDS", 0.1),
        patch("migrateit.clients._lock.LOCK_RETRY_SECONDS", 0.01),
    ):
        yield


@contextmanager
def _second_client(database: SupportedDatabase, temp_dir: Path) -> Generator[SqlClient[Any]]:
    with connection(database) as conn:
        config = MigrateItConfig(
            changelog=Changelog(
                version=1,
                database=database,
                migrations=[
                    Migration(
                        name=INITIAL_MIGRATION,
                        initial=True,
                        parents=(),
                    )
                ],
            ),
        )
        yield get_client(config, conn)


@pytest.mark.parametrize(
    ("name",),
    [
        (TEST_MIGRATIONS_TABLE,),
        ("table_a",),
        ("MIGRATEIT_CHANGELOG",),
        ("my_table_123",),
    ],
    ids=["deterministic", "different_a", "different_b", "edge"],
)
def test_compute_advisory_key(name: str) -> None:
    key = _PsqlLocker._compute_advisory_key(name)
    assert isinstance(key, int)
    assert key >= 0
    assert _PsqlLocker._compute_advisory_key(name) == key


@pytest.mark.unit
@pytest.mark.parametrize(
    ("database", "expected_locker_cls"),
    [
        (SupportedDatabase.POSTGRES, _PsqlLocker),
        (SupportedDatabase.MYSQL, _MySqlLocker),
        (SupportedDatabase.MARIADB, _MySqlLocker),
        (SupportedDatabase.ORACLE, _DBTableLocker),
        (SupportedDatabase.SQLITE, _DBTableLocker),
    ],
)
def test_database_lock_dispatches(database: SupportedDatabase, expected_locker_cls: type) -> None:
    mock_client = MagicMock()
    mock_client.database = database
    lock = DatabaseLock(mock_client, TEST_MIGRATIONS_TABLE)
    assert isinstance(lock._locker, expected_locker_cls)  # noqa: SLF001


def test_lock_acquire_and_release(client: SqlClient[Any]) -> None:
    with DatabaseLock(client, TEST_MIGRATIONS_TABLE) as lock:
        assert lock.is_locked
    assert not lock.is_locked


def test_lock_cannot_be_acquired_twice(client: SqlClient[Any], temp_dir: Path) -> None:
    if client.database in {SupportedDatabase.SQLITE}:
        pytest.skip("Skipped because of in-memory database usage")

    with _second_client(client.database, temp_dir) as second_client:
        lock = DatabaseLock(client, TEST_MIGRATIONS_TABLE)
        lock2 = DatabaseLock(second_client, TEST_MIGRATIONS_TABLE)

        lock._locker.acquire()
        assert lock.is_locked

        with pytest.raises(RuntimeError, match="Could not acquire lock"):
            lock2._locker.acquire()


def test_table_lock_cleanup_on_error(client: SqlClient[Any]) -> None:
    if client.database not in {SupportedDatabase.ORACLE, SupportedDatabase.SQLITE}:
        pytest.skip("This test is only for databases that lock using a database table")

    with pytest.raises(RuntimeError, match="simulated"):
        with DatabaseLock(client, TEST_MIGRATIONS_TABLE):
            raise RuntimeError("simulated")

    result = client.execute_for_one(f'SELECT COUNT(*) FROM "{TEST_MIGRATIONS_TABLE}_lock"')
    assert result is not None and result[0] == 0


def test_table_lock_unsafe_table_name(client: SqlClient[Any]) -> None:
    if client.database not in {SupportedDatabase.ORACLE, SupportedDatabase.SQLITE}:
        pytest.skip("This test is only for databases that lock using a database table")

    with pytest.raises(ValueError, match="Unsafe table name"):
        _DBTableLocker(client, "invalid-name")


@pytest.mark.unit
def test_table_lock_timeout() -> None:
    client = Mock()
    client.execute.side_effect = Exception("DB error")

    time_sequence = [0.0, 0.0, 0.2]
    with patch("time.monotonic", side_effect=time_sequence):
        lock = _DBTableLocker(client=client, table_name=TEST_MIGRATIONS_TABLE)

        with pytest.raises(RuntimeError, match="Could not acquire lock"):
            lock.acquire()
