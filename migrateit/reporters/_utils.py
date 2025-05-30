import contextlib
from typing import Any

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
SUBTLE = "\033[2m"
NORMAL = "\033[m"


def force_bytes(exc: Any) -> bytes:
    with contextlib.suppress(TypeError):
        return bytes(exc)
    with contextlib.suppress(Exception):
        return str(exc).encode()
    return f"<unprintable {type(exc).__name__} object>".encode()


def format_color(text: str, color: str, use_color_setting: bool) -> str:
    """Format text with color.

    Args:
        text - Text to be formatted with color if `use_color`
        color - The color start string
        use_color_setting - Whether or not to color
    """
    if use_color_setting:
        return f"{color}{text}{NORMAL}"
    else:
        return text
