from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Protocol, Self, cast, override

from migrateit.models.config import SupportedDatabase
from migrateit.models.connection import Connection
from migrateit.reporters.logs import logger

if TYPE_CHECKING:
    import sqlite3  # noqa: F401

    import oracledb  # noqa: F401
    import psycopg  # noqa: F401

    from migrateit.models.connection import _MySqlConnection  # noqa: F401

LOCK_TIMEOUT_SECONDS = 30
LOCK_RETRY_SECONDS = 0.5


class Locker(Protocol):
    def acquire(self) -> None: ...
    def release(self) -> None: ...


class _DummyLocker(Locker):
    def __init__(self, connection: Any, table_name: str) -> None:
        self._connection = connection
        self._table_name = table_name

    @override
    def acquire(self) -> None:
        pass

    @override
    def release(self) -> None:
        pass


class _PsqlLocker(Locker):
    def __init__(self, connection: psycopg.Connection, table_name: str) -> None:
        self._connection = connection
        self._table_name = table_name

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        self.release()

    @override
    def acquire(self) -> None:
        key = self._compute_advisory_key(self._table_name)
        query = "SELECT pg_advisory_lock(%s)"
        with self._connection.cursor() as cursor:
            cursor.execute(query, (key,))
        logger.debug("Acquired advisory lock for table %s (key=%d)", self._table_name, key)

    @override
    def release(self) -> None:
        key = self._compute_advisory_key(self._table_name)
        query = "SELECT pg_advisory_unlock(%s)"
        with self._connection.cursor() as cursor:
            cursor.execute(query, (key,))
        logger.debug("Released advisory lock for table %s (key=%d)", self._table_name, key)

    @staticmethod
    def _compute_advisory_key(table_name: str) -> int:
        """Compute a deterministic advisory lock key from the table name.

        Returns a non-negative 63-bit integer suitable for PostgreSQL's
        ``pg_advisory_lock`` (which expects a signed 64-bit int).
        """
        raw = int(table_name.encode("utf-8").hex(), 16)
        return raw % (2**63)


class _SqliteLocker(Locker):
    @property
    def lock_table(self) -> str:
        return f'"{self._table_name}_lock"'

    def __init__(self, connection: sqlite3.Connection, table_name: str) -> None:
        if not table_name.isidentifier():
            raise ValueError(f"Unsafe table name: {table_name}")
        self._connection = connection
        self._table_name = table_name

        self._connection.execute(f"CREATE TABLE IF NOT EXISTS {self.lock_table} (id INTEGER PRIMARY KEY)")
        self._connection.commit()

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        self.release()

    @override
    def acquire(self) -> None:
        end_time = time.monotonic() + LOCK_TIMEOUT_SECONDS
        while time.monotonic() < end_time:
            cursor = self._connection.execute(f"INSERT OR IGNORE INTO {self.lock_table} (id) VALUES (1)")
            if cursor.rowcount > 0:
                logger.debug("Acquired SQLite lock for table %s", self._table_name)
                return
            time.sleep(LOCK_RETRY_SECONDS)
        raise RuntimeError(f"Could not acquire lock for {self._table_name} within {LOCK_TIMEOUT_SECONDS}s")

    @override
    def release(self) -> None:
        self._connection.execute(f"DELETE FROM {self.lock_table} WHERE id = 1")
        self._connection.commit()
        logger.debug("Released SQLite lock for table %s", self._table_name)


class _MySqlLocker(Locker):
    @property
    def lock_name(self) -> str:
        return f'"{self._table_name}_lock"'

    def __init__(self, connection: _MySqlConnection, table_name: str) -> None:
        self._connection = connection
        self._table_name = table_name

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        self.release()

    @override
    def acquire(self) -> None:
        query = "SELECT GET_LOCK(%s, %s)"
        with self._connection.cursor() as cursor:
            cursor.execute(query, (self.lock_name, LOCK_TIMEOUT_SECONDS))
            result = cast(tuple[object, ...], cursor.fetchone())
            if result is None or result[0] == 0:
                raise RuntimeError(f"Could not acquire lock '{self.lock_name}' for {self._table_name}")
            cursor.fetchall()
        logger.debug("Acquired MySQL lock '%s' for table %s", self.lock_name, self._table_name)

    @override
    def release(self) -> None:
        query = "SELECT RELEASE_LOCK(%s)"
        with self._connection.cursor() as cursor:
            cursor.execute(query, (self.lock_name,))
            cursor.fetchall()
        logger.debug("Released MySQL lock '%s' for table %s", self.lock_name, self._table_name)


class _OracleLocker(Locker):
    @property
    def lock_table(self) -> str:
        return f'"{self._table_name}_lock"'

    def __init__(self, connection: oracledb.Connection, table_name: str) -> None:
        import oracledb

        if not table_name.isidentifier():
            raise ValueError(f"Unsafe table name: {table_name}")
        self._connection = connection
        self._table_name = table_name

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE TABLE {self.lock_table} (id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)"
                )
            self._connection.commit()
        except oracledb.DatabaseError as e:
            # ORA-00955: name is already used by an existing object
            if e.args[0].code == 955:
                self._connection.rollback()
            else:
                raise

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        self.release()

    @override
    def acquire(self) -> None:
        end_time = time.monotonic() + LOCK_TIMEOUT_SECONDS
        while time.monotonic() < end_time:
            with self._connection.cursor() as cursor:
                cursor.execute(f"INSERT INTO {self.lock_table} (id) VALUES (1)")
                if cursor.rowcount > 0:
                    self._connection.commit()
                    logger.debug("Acquired Oracle lock for table %s", self._table_name)
                    return
            time.sleep(LOCK_RETRY_SECONDS)
        raise RuntimeError(f"Could not acquire lock for {self._table_name} within {LOCK_TIMEOUT_SECONDS}s")

    @override
    def release(self) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {self.lock_table} WHERE id = 1")
        self._connection.commit()


class DatabaseLock:
    """Database-agnostic context manager for migration execution locks.

    Wraps the connection with the appropriate locking strategy for the
    connected database type. The lock is acquired on entry and released
    on exit (whether normal or via exception).
    """

    def __init__(self, connection: Connection, database: SupportedDatabase, table_name: str) -> None:
        self._connection = connection
        self._database = database
        self._table_name = table_name
        self._locker: Locker = self._build_locker()

    def __enter__(self) -> Self:
        self._locker.acquire()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        self._locker.release()

    def _build_locker(self) -> Locker:
        match self._database:
            case SupportedDatabase.MYSQL | SupportedDatabase.MARIADB:
                return _MySqlLocker(self._connection, self._table_name)  # type: ignore[arg-type]
            case SupportedDatabase.ORACLE:
                return _OracleLocker(self._connection, self._table_name)  # type: ignore[arg-type]
            case SupportedDatabase.POSTGRES:
                return _PsqlLocker(self._connection, self._table_name)  # type: ignore[arg-type]
            case SupportedDatabase.SQLITE:
                return _SqliteLocker(self._connection, self._table_name)  # type: ignore[arg-type]
            case _:  # pragma: no cover
                return _DummyLocker(self._connection, self._table_name)
