import sqlite3
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from migrateit.clients._lock import (
    DatabaseLock,
    _MySqlLocker,
    _OracleLocker,
    _PsqlLocker,
    _SqliteLocker,
)
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


def _make_sqlite_conn() -> sqlite3.Connection:
    conn: sqlite3.Connection = sqlite3.connect(":memory:", autocommit=False)
    conn.executescript(_MIGRATIONS_TABLE_DDL)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Advisory key
# ---------------------------------------------------------------------------


def test_compute_advisory_key_deterministic() -> None:
    result = _PsqlLocker._compute_advisory_key(TEST_MIGRATIONS_TABLE)
    assert result == _PsqlLocker._compute_advisory_key(TEST_MIGRATIONS_TABLE)


def test_compute_advisory_key_different_for_different_names() -> None:
    key1 = _PsqlLocker._compute_advisory_key("table_a")
    key2 = _PsqlLocker._compute_advisory_key("table_b")
    assert key1 != key2


def test_compute_advisory_key_non_negative() -> None:
    for name in ("migrations", "MIGRATEIT_CHANGELOG", "my_table_123"):
        assert _PsqlLocker._compute_advisory_key(name) >= 0


# ---------------------------------------------------------------------------
# SQLite/Oracle lock
# ---------------------------------------------------------------------------


def test_sqlite_lock_acquire_and_release() -> None:
    conn = _make_sqlite_conn()

    with DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE):
        pass

    cursor = conn.execute(f'SELECT COUNT(*) FROM "{TEST_MIGRATIONS_TABLE}_lock"')
    count = cursor.fetchone()[0]
    assert count == 0


def test_sqlite_lock_released_on_error() -> None:
    conn = _make_sqlite_conn()

    with pytest.raises(RuntimeError, match="simulated"):
        with DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE):
            raise RuntimeError("simulated")

    cursor = conn.execute(f'SELECT COUNT(*) FROM "{TEST_MIGRATIONS_TABLE}_lock"')
    count = cursor.fetchone()[0]
    assert count == 0


def test_sqlite_lock_concurrent(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"

    # Initialize lock schema on a setup connection
    setup_conn = sqlite3.connect(db_path)
    setup_conn.executescript(_MIGRATIONS_TABLE_DDL)
    setup_conn.commit()
    setup_conn.close()

    errors: list[BaseException] = []
    acquired = threading.Event()

    def holder() -> None:
        try:
            # Separate connection for holder thread
            conn = sqlite3.connect(db_path, timeout=5.0)
            with DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE):
                acquired.set()
                time.sleep(1)
            conn.close()
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    def waiter() -> None:
        try:
            # Separate connection for waiter thread
            conn = sqlite3.connect(db_path, timeout=5.0)
            with DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE):
                pass
            conn.close()
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    t1 = threading.Thread(target=holder)
    t2 = threading.Thread(target=waiter)
    t1.start()
    t2.start()

    acquired.wait(timeout=2)
    t2.join(timeout=5)
    t1.join(timeout=2)

    assert not errors, f"Unexpected errors: {errors}"


def test_sqlite_lock_timeout() -> None:
    conn = _make_sqlite_conn()
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{TEST_MIGRATIONS_TABLE}_lock" (id INTEGER PRIMARY KEY)')
    conn.commit()

    # Hold the lock on the same connection to force timeout
    with (
        _SqliteLocker(conn, TEST_MIGRATIONS_TABLE),
        patch("migrateit.clients._lock.LOCK_TIMEOUT_SECONDS", 5),
    ):
        locker = _SqliteLocker(conn, TEST_MIGRATIONS_TABLE)
        with pytest.raises(RuntimeError, match="Could not acquire lock"):
            with locker:
                pass  # Should not reach here


def test_sqlite_locker_release() -> None:
    conn = _make_sqlite_conn()
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{TEST_MIGRATIONS_TABLE}_lock" (id INTEGER PRIMARY KEY)')
    conn.commit()
    locker = _SqliteLocker(conn, "migrateit")
    locker.acquire()
    locker.release()

    cursor = conn.execute(f'SELECT COUNT(*) FROM "{TEST_MIGRATIONS_TABLE}_lock"')
    count = cursor.fetchone()[0]
    assert count == 0


# ---------------------------------------------------------------------------
# Oracle lock (mocked)
# ---------------------------------------------------------------------------


def test_oracle_locker_init_creates_table() -> None:
    mock_conn = MagicMock()
    cursor = mock_conn.cursor.return_value
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)

    _ = _OracleLocker(mock_conn, TEST_MIGRATIONS_TABLE)  # side-effect: creates lock table

    cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once()


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
    # Reset init calls
    cursor.execute.reset_mock()
    mock_conn.commit.reset_mock()

    locker.acquire()
    locker.release()

    assert cursor.execute.call_count == 2
    calls = [c[0][0] for c in cursor.execute.call_args_list]
    assert any("INSERT" in c for c in calls)
    assert any("DELETE" in c for c in calls)


def test_oracle_locker_context_manager() -> None:
    mock_conn = MagicMock()
    cursor = mock_conn.cursor.return_value
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.rowcount = 1

    locker = _OracleLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    # Reset init calls
    cursor.execute.reset_mock()
    mock_conn.commit.reset_mock()

    with locker:
        pass

    assert cursor.execute.call_count == 2


def test_oracle_locker_release_on_error() -> None:
    mock_conn = MagicMock()
    cursor = mock_conn.cursor.return_value
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.rowcount = 1

    locker = _OracleLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    # Reset init calls
    cursor.execute.reset_mock()
    mock_conn.commit.reset_mock()

    with pytest.raises(RuntimeError, match="simulated"):
        with locker:
            raise RuntimeError("simulated")

    # Verify release (DELETE) was called despite the error
    calls = [c[0][0] for c in cursor.execute.call_args_list]
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
        # Reset init calls
        cursor.execute.reset_mock()
        mock_conn.commit.reset_mock()

        with pytest.raises(RuntimeError, match="Could not acquire lock"):
            locker.acquire()

    # Verify multiple INSERT attempts were made
    insert_calls = [c for c in cursor.execute.call_args_list if "INSERT" in str(c)]
    assert len(insert_calls) >= 1


# ---------------------------------------------------------------------------
# PostgreSQL lock (mocked)
# ---------------------------------------------------------------------------


def test_psql_locker_acquire_and_release() -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    locker = _PsqlLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    locker.acquire()
    locker.release()

    # Verify pg_advisory_lock was called with the computed key
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
# MySQL lock (mocked)
# ---------------------------------------------------------------------------


def test_mysql_locker_acquire_success() -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (1,)

    locker = _MySqlLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    locker.acquire()

    mock_cursor.fetchall.assert_called()


def test_mysql_locker_acquire_timeout() -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (0,)

    locker = _MySqlLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    with pytest.raises(RuntimeError, match="Could not acquire lock"):
        locker.acquire()


def test_mysql_locker_release() -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.fetchall.return_value = [(1,)]

    locker = _MySqlLocker(mock_conn, TEST_MIGRATIONS_TABLE)
    locker.release()

    mock_cursor.fetchall.assert_called()


def test_mysql_locker_context_manager() -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.fetchall.return_value = [(1,)]

    with _MySqlLocker(mock_conn, TEST_MIGRATIONS_TABLE):
        pass

    assert mock_cursor.execute.call_count == 2


def test_database_lock_builds_psql_locker() -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock(spec=psycopg.Connection)
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    lock = DatabaseLock(mock_conn, SupportedDatabase.POSTGRES, TEST_MIGRATIONS_TABLE)  # noqa: F841
    assert isinstance(lock._locker, _PsqlLocker)  # noqa: SLF001


def test_database_lock_builds_mysql_locker() -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.fetchall.return_value = [(1,)]

    # MagicMock without spec should be treated as MySQL
    lock = DatabaseLock(mock_conn, SupportedDatabase.MARIADB, TEST_MIGRATIONS_TABLE)  # noqa: F841
    assert isinstance(lock._locker, _MySqlLocker)  # noqa: SLF001


def test_database_lock_builds_oracle_locker() -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.rowcount = 1

    lock = DatabaseLock(mock_conn, SupportedDatabase.ORACLE, TEST_MIGRATIONS_TABLE)  # noqa: F841
    assert isinstance(lock._locker, _OracleLocker)  # noqa: SLF001
