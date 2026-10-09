import sqlite3
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from migrateit.clients._lock import (
    LOCK_NAME,
    DatabaseLock,
    _MySqlLocker,
    _PsqlLocker,
    _SqliteLocker,
)
from migrateit.models.changelog import SupportedDatabase
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
# SQLite lock
# ---------------------------------------------------------------------------


def test_sqlite_lock_acquire_and_release() -> None:
    conn = _make_sqlite_conn()

    with DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE):
        pass

    cursor = conn.execute('SELECT COUNT(*) FROM "migrateit_lock"')
    count = cursor.fetchone()[0]
    assert count == 0


def test_sqlite_lock_released_on_error() -> None:
    conn = _make_sqlite_conn()

    with pytest.raises(RuntimeError, match="simulated"):
        with DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE):
            raise RuntimeError("simulated")

    cursor = conn.execute('SELECT COUNT(*) FROM "migrateit_lock"')
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


def test_sqlite_lock_table_created() -> None:
    conn = _make_sqlite_conn()

    DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE)

    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        ("migrateit_lock",),
    )
    assert cursor.fetchone() is not None


def test_sqlite_lock_repeated_acquisitions() -> None:
    conn = _make_sqlite_conn()

    for _ in range(3):
        with DatabaseLock(conn, SupportedDatabase.SQLITE, TEST_MIGRATIONS_TABLE):
            pass

    cursor = conn.execute('SELECT COUNT(*) FROM "migrateit_lock"')
    count = cursor.fetchone()[0]
    assert count == 0


def test_sqlite_lock_timeout() -> None:
    conn = _make_sqlite_conn()
    conn.execute('CREATE TABLE IF NOT EXISTS "migrateit_lock" (id INTEGER PRIMARY KEY)')
    conn.commit()

    # Hold the lock on the same connection to force timeout
    with (
        _SqliteLocker(conn, "migrateit"),
        patch("migrateit.clients._lock.LOCK_TIMEOUT_SECONDS", 5),
    ):
        locker = _SqliteLocker(conn, "migrateit")
        with pytest.raises(RuntimeError, match="Could not acquire lock"):
            with locker:
                pass  # Should not reach here


def test_sqlite_locker_release() -> None:
    conn = _make_sqlite_conn()
    conn.execute('CREATE TABLE IF NOT EXISTS "migrateit_lock" (id INTEGER PRIMARY KEY)')
    conn.commit()
    locker = _SqliteLocker(conn, "migrateit")
    locker.acquire()
    locker.release()

    cursor = conn.execute('SELECT COUNT(*) FROM "migrateit_lock"')
    count = cursor.fetchone()[0]
    assert count == 0


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
    with pytest.raises(RuntimeError, match=f"Could not acquire lock '{LOCK_NAME}'"):
        locker.acquire()


def test_mysql_locker_release() -> None:
    """Test MySQL lock release."""
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
