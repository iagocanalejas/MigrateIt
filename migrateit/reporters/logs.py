import contextlib
import logging
from collections.abc import Generator

from ._utils import RED, YELLOW, format_color
from .output import write_line

logger = logging.getLogger("migrateit")

LOG_LEVEL_COLORS = {
    "DEBUG": "",
    "INFO": "",
    "WARNING": YELLOW,
    "ERROR": RED,
}


class LoggingHandler(logging.Handler):
    def __init__(self, use_color: bool) -> None:
        super().__init__()
        self.use_color = use_color

    def emit(self, record: logging.LogRecord) -> None:
        level_msg = format_color(
            f"[{record.levelname}]",
            LOG_LEVEL_COLORS[record.levelname],
            self.use_color,
        )
        write_line(f"{level_msg} {record.getMessage()}")


@contextlib.contextmanager
def logging_handler(use_color: bool) -> Generator[None]:
    handler = LoggingHandler(use_color)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield
    finally:
        logger.removeHandler(handler)
