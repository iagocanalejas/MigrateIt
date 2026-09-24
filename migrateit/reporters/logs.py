import contextlib
import logging
import traceback
from collections.abc import Generator

from ._utils import BLUE, RED, YELLOW, format_color
from .output import write_line

logger = logging.getLogger("migrateit")

LOG_LEVEL_COLORS: dict[str, str] = {
    "DEBUG": BLUE,
    "INFO": "",
    "WARNING": YELLOW,
    "ERROR": RED,
    "CRITICAL": RED,
}

DEFAULT_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


class LoggingHandler(logging.Handler):
    """A logging.Handler that writes colored log messages to the terminal."""

    def __init__(self, use_color: bool = True, fmt: str = DEFAULT_LOG_FORMAT) -> None:
        super().__init__()
        self.use_color = use_color
        self.fmt = fmt
        self.formatter: logging.Formatter = logging.Formatter(fmt)  # pyright: ignore

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.formatter.format(record)
            # Extract level for coloring
            levelname = record.levelname
            color = LOG_LEVEL_COLORS.get(levelname, "")
            if self.use_color and color:
                msg = format_color(msg, color, self.use_color)
            # Append traceback if exc_info is available
            if record.exc_info and record.exc_info[0] is not None:
                msg += "\n" + "".join(traceback.format_exception(*record.exc_info))
            write_line(msg)
        except Exception:  # noqa: BLE001
            self.handleError(record)


@contextlib.contextmanager
def logging_handler(
    use_color: bool = True,
    level: int = logging.INFO,
    fmt: str = DEFAULT_LOG_FORMAT,
) -> Generator[None]:
    """Context manager that installs a LoggingHandler on the migrateit logger.

    Args:
        use_color: Whether to colorize log messages.
        level: The logging level to filter on (default: INFO).
        fmt: Log message format string (supports standard logging keys).
    """
    handler = LoggingHandler(use_color=use_color, fmt=fmt)
    logger.addHandler(handler)
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.removeHandler(handler)
