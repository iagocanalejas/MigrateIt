from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from migrateit.clients._lock import DatabaseLock, _MySqlLocker, _OracleLocker, _PsqlLocker, _SqliteLocker
from migrateit.models.config import SupportedDatabase
from tests.conftest import TEST_MIGRATIONS_TABLE

# SQLite DDL for the migrations table (matches the actual schema)
_MIGRATIONS_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS "{TEST_MIGRATIONS_TABLE}" (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_name VARCHAR(255) UNIQUE NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_hash VARCHAR(64) NOT NULL,
    squashed BOOLEAN DEFAULT 0
);
"""


# ---------------------------------------------------------------------------
# Advisory key (static utility)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# _SqliteLocker — real SQLite, no mocks
# ---------------------------------------------------------------------------


def test_sqlite_lock_error_cleanup(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(_MIGRATIONS_TABLE_DDL)

    conn = sqlite3.connect(db_path, timeout=5.0)
    with pytest.raises(RuntimeError, match="simulated"):
        with DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE):
            raise RuntimeError("simulated")
    conn.close()

    with sqlite3.connect(db_path) as conn:
        count = conn.execute(f'SELECT COUNT(*) FROM "{TEST_MIGRATIONS_TABLE}_lock"').fetchone()[0]
    assert count == 0


def test_sqlite_lock_concurrent(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(_MIGRATIONS_TABLE_DDL)

    errors: list[BaseException] = []
    acquired = threading.Event()

    def holder() -> None:
        try:
            conn = sqlite3.connect(db_path, timeout=5.0)
            with DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE):
                acquired.set()
                time.sleep(0.1)
            conn.close()
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    def waiter() -> None:
        try:
            conn = sqlite3.connect(db_path, timeout=5.0)
            with DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE):
                pass
            conn.close()
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    t1, t2 = threading.Thread(target=holder), threading.Thread(target=waiter)
    t1.start()
    t2.start()
    acquired.wait(timeout=2)
    t2.join(timeout=5)
    t1.join(timeout=2)

    assert not errors, f"Unexpected errors: {errors}"


def test_sqlite_locker_context_manager() -> None:
    conn: sqlite3.Connection = sqlite3.connect(":memory:", autocommit=False)
    conn.executescript(_MIGRATIONS_TABLE_DDL)
    conn.commit()

    with _SqliteLocker(conn, TEST_MIGRATIONS_TABLE):
        pass

    count = conn.execute(f'SELECT COUNT(*) FROM "{TEST_MIGRATIONS_TABLE}_lock"').fetchone()[0]
    assert count == 0


def test_sqlite_locker_timeout(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(f'CREATE TABLE "{TEST_MIGRATIONS_TABLE}_lock" (id INTEGER PRIMARY KEY)')
    conn.execute(f'INSERT INTO "{TEST_MIGRATIONS_TABLE}_lock" (id) VALUES (1)')
    conn.commit()
    conn.close()

    waiter = sqlite3.connect(db_path, timeout=5.0)
    with (
        patch("migrateit.clients._lock.LOCK_TIMEOUT_SECONDS", 0.1),
        patch("migrateit.clients._lock.LOCK_RETRY_SECONDS", 0.01),
    ):
        locker = _SqliteLocker(waiter, TEST_MIGRATIONS_TABLE)
        with pytest.raises(RuntimeError, match="Could not acquire lock"):
            with locker:
                pass  # pragma: no cover
    waiter.close()


# ---------------------------------------------------------------------------
# _OracleLocker — mocked
# ---------------------------------------------------------------------------


def test_oracle_locker_init_success() -> None:
    mock_conn = MagicMock()
    cursor = mock_conn.cursor.return_value
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)

    _OracleLocker(mock_conn, TEST_MIGRATIONS_TABLE)

    mock_conn.commit.assert_called_once()
    mock_conn.rollback.assert_not_called()


def test_oracle_locker_init_already_exists() -> None:
    import oracledb

    mock_conn = MagicMock()
    cursor = mock_conn.cursor.return_value
    error = oracledb.DatabaseError()
    error.args = (MagicMock(),)
    error.args[0].code = 955  # ORA-00955: name already used
    cursor.__enter__ = MagicMock(side_effect=error)
    cursor.__exit__ = MagicMock(return_value=False)

    _OracleLocker(mock_conn, TEST_MIGRATIONS_TABLE)

    mock_conn.rollback.assert_called_once()
    mock_conn.commit.assert_not_called()


def test_oracle_locker_init_raises_unhandled() -> None:
    import oracledb

    mock_conn = MagicMock()
    cursor = mock_conn.cursor.return_value
    error = oracledb.DatabaseError()
    error.args = (MagicMock(),)
    error.args[0].code = 942  # ORA-00942: table does not exist
    cursor.__enter__ = MagicMock(side_effect=error)
    cursor.__exit__ = MagicMock(return_value=False)

    with pytest.raises(oracledb.DatabaseError):
        _OracleLocker(mock_conn, TEST_MIGRATIONS_TABLE)

    mock_conn.rollback.assert_not_called()
    mock_conn.commit.assert_not_called()


def test_oracle_locker_acquire_and_release() -> None:
    mock_conn = MagicMock()
    cursor = mock_conn.cursor.return_value
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.rowcount = 1

    locker = _OracleLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    cursor.execute.reset_mock()
    mock_conn.commit.reset_mock()

    locker.acquire()
    locker.release()

    assert cursor.execute.call_count == 2
    calls = [c[0][0] for c in cursor.execute.call_args_list]
    assert any("INSERT" in c for c in calls)
    assert any("DELETE" in c for c in calls)


def test_oracle_locker_timeout() -> None:
    mock_conn = MagicMock()
    cursor = mock_conn.cursor.return_value
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.rowcount = 0

    with (
        patch("migrateit.clients._lock.LOCK_TIMEOUT_SECONDS", 0.1),
        patch("migrateit.clients._lock.LOCK_RETRY_SECONDS", 0.01),
    ):
        locker = _OracleLocker(mock_conn, TEST_MIGRATIONS_TABLE)
        cursor.execute.reset_mock()

        with pytest.raises(RuntimeError, match="Could not acquire lock"):
            locker.acquire()


def test_oracle_locker_release_on_error() -> None:
    mock_conn = MagicMock()
    cursor = mock_conn.cursor.return_value
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.rowcount = 1

    locker = _OracleLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    cursor.execute.reset_mock()

    with pytest.raises(RuntimeError, match="simulated"):
        with locker:
            raise RuntimeError("simulated")

    calls = [c[0][0] for c in cursor.execute.call_args_list]
    assert any("DELETE" in c for c in calls)


# ---------------------------------------------------------------------------
# _PsqlLocker — mocked
# ---------------------------------------------------------------------------


def test_psql_locker_acquire_and_release() -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    locker = _PsqlLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    locker.acquire()
    locker.release()

    calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
    assert len(calls) == 2
    assert "pg_advisory_lock" in str(calls[0])
    assert "pg_advisory_unlock" in str(calls[1])


def test_psql_locker_context_manager() -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with _PsqlLocker(mock_conn, TEST_MIGRATIONS_TABLE):
        pass

    assert mock_cursor.execute.call_count == 2


# ---------------------------------------------------------------------------
# _MySqlLocker — mocked
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fetchone_result", "expect_acquire"),
    [
        ((1,), True),
        ((0,), False),
    ],
)
def test_mysql_locker_acquire(fetchone_result: tuple[int], expect_acquire: bool) -> None:
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = fetchone_result

    locker = _MySqlLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    if expect_acquire:
        locker.acquire()
        mock_cursor.fetchall.assert_called()
    else:
        with pytest.raises(RuntimeError, match="Could not acquire lock"):
            locker.acquire()


def test_mysql_locker_context_manager() -> None:
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.fetchall.return_value = [(1,)]

    with _MySqlLocker(mock_conn, TEST_MIGRATIONS_TABLE):
        pass

    assert mock_cursor.execute.call_count == 2


def test_mysql_locker_release() -> None:
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.fetchall.return_value = [(1,)]

    locker = _MySqlLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    locker.release()
    mock_cursor.fetchall.assert_called()


# ---------------------------------------------------------------------------
# DatabaseLock — factory dispatch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("database", "expected_locker_cls"),
    [
        (SupportedDatabase.POSTGRES, _PsqlLocker),
        (SupportedDatabase.MYSQL, _MySqlLocker),
        (SupportedDatabase.MARIADB, _MySqlLocker),
        (SupportedDatabase.ORACLE, _OracleLocker),
        (SupportedDatabase.SQLITE, _SqliteLocker),
    ],
)
def test_database_lock_dispatches(database: SupportedDatabase, expected_locker_cls: type) -> None:
    mock_conn = MagicMock()
    lock = DatabaseLock(mock_conn, database, TEST_MIGRATIONS_TABLE)
    assert isinstance(lock._locker, expected_locker_cls)  # noqa: SLF001
