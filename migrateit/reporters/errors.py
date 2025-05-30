import contextlib
import functools
import os
import sys
import traceback
from collections.abc import Generator
from typing import IO

import migrateit.constants as C

from ._utils import force_bytes
from .output import write_line, write_line_b


class FatalError(RuntimeError):
    pass


@contextlib.contextmanager
def error_handler() -> Generator[None]:
    try:
        yield
    except (Exception, KeyboardInterrupt) as e:
        if isinstance(e, FatalError):
            msg, ret_code = "An error has occurred", 1
        elif isinstance(e, KeyboardInterrupt):
            msg, ret_code = "Interrupted (^C)", 130
        else:
            msg, ret_code = "An unexpected error has occurred", 3
        _log_and_exit(msg, ret_code, e, traceback.format_exc())


def _log_and_exit(
    msg: str,
    ret_code: int,
    exc: BaseException,
    formatted: str,
) -> None:
    error_msg = f"{msg}: {type(exc).__name__}: ".encode() + force_bytes(exc)
    write_line_b(error_msg)

    migrateitdir = os.path.realpath(C.MIGRATEIT_ROOT_DIR)
    log_path = os.path.join(migrateitdir, "migrateit.log")
    with contextlib.ExitStack() as ctx:
        if os.access(migrateitdir, os.W_OK):
            write_line(f"Check the log at {log_path}")
            log: IO[bytes] = ctx.enter_context(open(log_path, "wb"))
        else:  # pragma: win32 no cover
            write_line(f"Failed to write to log at {log_path}")
            log = sys.stdout.buffer

        _log_line = functools.partial(write_line, stream=log)
        _log_line_b = functools.partial(write_line_b, stream=log)

        _log_line("### version information")
        _log_line()
        _log_line("```")
        _log_line(f"migrateit version: {C.VERSION}")
        _log_line("sys.version:")
        for line in sys.version.splitlines():
            _log_line(f"    {line}")
        _log_line(f"sys.executable: {sys.executable}")
        _log_line(f"os.name: {os.name}")
        _log_line(f"sys.platform: {sys.platform}")
        _log_line("```")
        _log_line()

        _log_line("### error information")
        _log_line()
        _log_line("```")
        _log_line_b(error_msg)
        _log_line("```")
        _log_line()
        _log_line("```")
        _log_line(formatted.rstrip())
        _log_line("```")
    raise SystemExit(ret_code)
