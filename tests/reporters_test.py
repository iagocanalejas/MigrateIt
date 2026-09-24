import logging
import sys
from unittest.mock import MagicMock, patch

import pytest
from psycopg import ProgrammingError

from migrateit.models.migration import Migration, MigrationStatus
from migrateit.reporters import (
    GREEN,
    NORMAL,
    RED,
    SUBTLE,
    YELLOW,
    FatalError,
    LoggingHandler,
    error_handler,
    force_bytes,
    format_color,
    logging_handler,
    pretty_print_sql_error,
    print_dag,
    print_list,
    write_line,
    write_line_b,
)
from migrateit.reporters._utils import BLUE

# --- TestForceBytes ---


def test_force_bytes_string() -> None:
    assert force_bytes("hello") == b"hello"


def test_force_bytes_int() -> None:
    # bytes(42) returns 42 null bytes, force_bytes uses bytes() first
    assert len(force_bytes(42)) == 42


def test_force_bytes_none() -> None:
    assert force_bytes(None) == b"None"


def test_force_bytes_unprintable() -> None:
    class Unprintable:
        def __str__(self) -> str:
            raise RuntimeError("cannot stringify")

    result = force_bytes(Unprintable())
    assert result == b"<unprintable Unprintable object>"


def test_force_bytes_exception() -> None:
    try:
        raise ValueError("test error")
    except Exception as e:
        assert force_bytes(e) == b"test error"


# --- TestFormatColor ---


def test_format_color_with_color() -> None:
    result = format_color("hello", RED, True)
    assert result == f"{RED}hello{NORMAL}"


def test_format_color_without_color() -> None:
    result = format_color("hello", RED, False)
    assert result == "hello"


def test_format_color_all_colors() -> None:
    for color in [RED, GREEN, YELLOW, BLUE, SUBTLE, NORMAL]:
        result = format_color("test", color, True)
        assert color in result


# --- TestPrintDag ---


@patch("migrateit.reporters.output.write_line")
def test_print_dag_with_children(mock_write: MagicMock) -> None:
    m2 = Migration(name="0002_child.sql", parents=["0001_init.sql"])
    children: dict[str, list[Migration]] = {"0001_init.sql": [m2], "0002_child.sql": []}
    status_map: dict[str, MigrationStatus] = {
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_child.sql": MigrationStatus.NOT_APPLIED,
    }
    print_dag("0001_init.sql", children, status_map, seen=set())
    assert mock_write.call_count > 1


@patch("migrateit.reporters.output.write_line")
def test_print_dag_seen_marker(mock_write: MagicMock) -> None:
    status_map: dict[str, MigrationStatus] = {"0001_init.sql": MigrationStatus.APPLIED}
    print_dag("0001_init.sql", {}, status_map, level=0, seen={"0001_init.sql"})
    last_call = mock_write.call_args_list[-1][0][0]
    assert "(*)" in last_call


# --- TestPrintList ---


@patch("migrateit.reporters.output.write_line")
def test_print_list(mock_write: MagicMock) -> None:
    children: dict[str, list[Migration]] = {"0001_init.sql": [], "0002_test.sql": []}
    status_map: dict[str, MigrationStatus] = {
        "0001_init.sql": MigrationStatus.APPLIED,
        "0002_test.sql": MigrationStatus.NOT_APPLIED,
    }
    print_list(children, status_map)
    assert mock_write.call_count == 2


@patch("migrateit.reporters.output.write_line")
def test_print_list_empty(mock_write: MagicMock) -> None:
    print_list({}, {})
    mock_write.assert_not_called()


# --- TestWriteLine ---


@patch("migrateit.reporters.output.write_line_b")
def test_write_line_calls_write_line_b(mock_write: MagicMock) -> None:
    write_line("hello")
    mock_write.assert_called_once_with(b"hello")


# --- TestWriteLineB ---


def test_write_line_b_explicit_stream() -> None:
    mock_stream = MagicMock()
    write_line_b(b"hello", stream=mock_stream)
    calls = [c[0][0] for c in mock_stream.write.call_args_list]
    assert b"hello" in calls
    assert b"\n" in calls
    mock_stream.flush.assert_called()


# --- TestFatalError ---


def test_fatal_error_is_runtime_error() -> None:
    err = FatalError("test")
    assert isinstance(err, RuntimeError)
    assert str(err) == "test"


def test_fatal_error_no_message() -> None:
    err = FatalError()
    assert isinstance(err, RuntimeError)


# --- TestErrorHandler ---


def test_error_handler_catches_fatal() -> None:
    with patch("migrateit.reporters.errors._log_and_exit", side_effect=SystemExit(1)):
        with pytest.raises(SystemExit):
            with error_handler():
                raise FatalError("fatal test")


def test_error_handler_catches_keyboard_interrupt() -> None:
    with patch("migrateit.reporters.errors._log_and_exit", side_effect=SystemExit(130)):
        with pytest.raises(SystemExit):
            with error_handler():
                raise KeyboardInterrupt()


def test_error_handler_catches_generic_exception() -> None:
    with patch("migrateit.reporters.errors._log_and_exit", side_effect=SystemExit(3)):
        with pytest.raises(SystemExit):
            with error_handler():
                raise ValueError("generic error")


def test_error_handler_no_exception() -> None:
    with patch("migrateit.reporters.errors._log_and_exit") as mock_log:
        with error_handler():
            pass
        mock_log.assert_not_called()


# --- TestLoggingHandler ---


@pytest.mark.parametrize(
    "level, msg",
    [
        (logging.INFO, "info message"),
        (logging.ERROR, "error message"),
        (logging.WARNING, "warn message"),
    ],
)
def test_logging_handler_emits(level: int, msg: str) -> None:
    handler = LoggingHandler(use_color=False)
    record = logging.LogRecord(
        name="test",
        level=level,
        pathname="",
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None,
    )
    with patch("migrateit.reporters.logs.write_line") as mock_write:
        handler.emit(record)
        mock_write.assert_called_once()
        assert msg in mock_write.call_args[0][0]


def test_logging_handler_color_enabled() -> None:
    handler = LoggingHandler(use_color=True)
    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname="",
        lineno=0,
        msg="colored",
        args=(),
        exc_info=None,
    )
    with patch("migrateit.reporters.logs.write_line") as mock_write:
        handler.emit(record)
        call_args = mock_write.call_args[0][0]
        assert RED in call_args


# --- TestLoggingContextManager ---


def test_logging_handler_adds_removes_handler() -> None:
    logger = logging.getLogger("migrateit")
    initial_handlers = len(logger.handlers)
    with logging_handler(use_color=True):
        assert len(logger.handlers) > initial_handlers
    assert len(logger.handlers) == initial_handlers


@patch("migrateit.reporters.logs.write_line")
def test_logging_handler_context_emits(mock_write: MagicMock) -> None:
    logger = logging.getLogger("migrateit")
    with logging_handler(use_color=False):
        logger.info("test log message")
    mock_write.assert_called()


# --- TestPrettyPrintSqlError ---


def test_pretty_print_sql_error() -> None:
    with patch("migrateit.reporters.output.write_line") as mock_write:
        error = ProgrammingError('syntax error at or near "SEL"')
        pretty_print_sql_error(error, "SEL * FROM foo;")
        assert mock_write.call_count >= 3


def test_pretty_print_sql_error_with_position() -> None:
    with patch("migrateit.reporters.output.write_line") as mock_write:
        error = ProgrammingError('syntax error at or near "SEL" POSITION: 1')
        pretty_print_sql_error(error, "SEL * FROM foo;")
        calls = [c[0][0] for c in mock_write.call_args_list]
        assert any("^" in c for c in calls)


# --- TestLoggingHandlerFormat ---


def test_logging_handler_format_with_custom_fmt() -> None:
    handler = LoggingHandler(use_color=False, fmt="CUSTOM: %(message)s")
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="hello",
        args=(),
        exc_info=None,
    )
    with patch("migrateit.reporters.logs.write_line") as mock_write:
        handler.emit(record)
        call_args = mock_write.call_args[0][0]
        assert "CUSTOM:" in call_args
        assert "hello" in call_args


def test_logging_handler_format_includes_logger_name() -> None:
    handler = LoggingHandler(use_color=False)
    record = logging.LogRecord(
        name="migrateit.cli",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="run command",
        args=(),
        exc_info=None,
    )
    with patch("migrateit.reporters.logs.write_line") as mock_write:
        handler.emit(record)
        call_args = mock_write.call_args[0][0]
        assert "migrateit.cli" in call_args


def test_logging_handler_format_includes_timestamp() -> None:
    handler = LoggingHandler(use_color=False)
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="hello",
        args=(),
        exc_info=None,
    )
    with patch("migrateit.reporters.logs.write_line") as mock_write:
        handler.emit(record)
        call_args = mock_write.call_args[0][0]
        # asctime format: YYYY-MM-DD HH:MM:SS,mmm
        assert "-" in call_args and ":" in call_args


def test_logging_handler_exc_info_appends_traceback() -> None:
    handler = LoggingHandler(use_color=False)
    try:
        raise ValueError("test error")
    except ValueError:
        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname="",
        lineno=0,
        msg="error occurred",
        args=(),
        exc_info=exc_info,
    )
    with patch("migrateit.reporters.logs.write_line") as mock_write:
        handler.emit(record)
        call_args = mock_write.call_args[0][0]
        assert "error occurred" in call_args
        assert "ValueError" in call_args
        assert "test error" in call_args


def test_logging_handler_exc_info_none_skips_traceback() -> None:
    handler = LoggingHandler(use_color=False)
    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname="",
        lineno=0,
        msg="error occurred",
        args=(),
        exc_info=None,
    )
    with patch("migrateit.reporters.logs.write_line") as mock_write:
        handler.emit(record)
        call_args = mock_write.call_args[0][0]
        assert "error occurred" in call_args
        assert "Traceback" not in call_args


def test_logging_handler_critical_color() -> None:
    handler = LoggingHandler(use_color=True)
    record = logging.LogRecord(
        name="test",
        level=logging.CRITICAL,
        pathname="",
        lineno=0,
        msg="critical msg",
        args=(),
        exc_info=None,
    )
    with patch("migrateit.reporters.logs.write_line") as mock_write:
        handler.emit(record)
        call_args = mock_write.call_args[0][0]
        assert RED in call_args


def test_logging_handler_sets_custom_level() -> None:
    logger = logging.getLogger("migrateit")
    with logging_handler(level=logging.DEBUG):
        assert logger.level == logging.DEBUG
