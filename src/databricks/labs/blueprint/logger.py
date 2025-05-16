"""A nice formatter for logging. It uses colors and bold text if the console supports it."""

import logging
import sys
from typing import TextIO


class NiceFormatter(logging.Formatter):
    """A nice formatter for logging. It uses colors and bold text if the console supports it."""

    # TODO: Actually detect if the console supports colors. Currently, it just assumes that it does.

    BOLD = "\033[1m"
    RESET = "\033[0m"
    GREEN = "\033[32m"
    BLACK = "\033[30m"
    CYAN = "\033[36m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    MAGENTA = "\033[35m"
    GRAY = "\033[90m"

    colors: bool
    """Whether this formatter is formatting with colors or not."""

    _levels: dict[int, str]
    """The colorized level names for each logging level."""

    _msg_colors: dict[int, str]
    """The color codes to use for rendering the message text depending on the logging level."""

    def __init__(self, *, probe_tty: bool = False, stream: TextIO = sys.stdout) -> None:
        """Create a new instance of the formatter.

        Args:
            stream: the output stream to which the formatter will write, used to check if it is a console.
            probe_tty: If true, the formatter will enable color support if the output stream appears to be a console.
        """
        super().__init__(fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s", datefmt="%H:%M:%S")
        # Used to colorize the level names.
        self._levels = {
            logging.DEBUG: self._bold(f"{self.CYAN}   DEBUG"),
            logging.INFO: self._bold(f"{self.GREEN}    INFO"),
            logging.WARNING: self._bold(f"{self.YELLOW} WARNING"),
            logging.ERROR: self._bold(f"{self.RED}   ERROR"),
            logging.CRITICAL: self._bold(f"{self.MAGENTA}CRITICAL"),
        }
        # Used to colorize the message text. (These are prefixes: after the message text, the color is reset.)
        self._msg_colors = {
            logging.DEBUG: self.GRAY,
            logging.INFO: self.BOLD,
            logging.WARNING: self.BOLD,
            logging.ERROR: f"{self.BOLD}{self.RED}",
            logging.CRITICAL: f"{self.BOLD}{self.RED}",
        }
        # show colors in runtime, github actions, and while debugging
        self.colors = stream.isatty() if probe_tty else True

    def _bold(self, text: str) -> str:
        """Return text in bold."""
        return f"{self.BOLD}{text}{self.RESET}"

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record. If colors are enabled, use them."""
        if not self.colors:
            return super().format(record)
        timestamp = self.formatTime(record, datefmt="%H:%M:%S")
        level = self._levels[record.levelno]

        # databricks.labs.ucx.foo.bar -> d.l.u.foo.bar
        module_split = record.name.split(".")
        abbreviated = [c[:1] for c in module_split[:-2]]  # abbreviate all but the last two components
        as_is = module_split[-2:]  # keep the last two components as-is
        name = ".".join([*abbreviated, *as_is])

        msg = record.getMessage()
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if not msg.endswith("\n"):
                msg += "\n"
            msg += record.exc_text
        if record.stack_info:
            if not msg.endswith("\n"):
                msg += "\n"
            msg += self.formatStack(record.stack_info)

        color_marker = self._msg_colors[record.levelno]

        thread_name = f"[{record.threadName}]" if record.threadName != "MainThread" else ""
        return f"{self.GRAY}{timestamp}{self.RESET} {level} {color_marker}[{name}]{thread_name} {msg}{self.RESET}"


def install_logger(
    level: int | str = logging.DEBUG, *, stream: TextIO = sys.stderr, root: logging.Logger = logging.root
) -> logging.StreamHandler:
    """Install a console logger with a nice formatter.

    The root logger will be modified:

     - Its logging level will be left as-is.
     - All existing handlers will be removed.
     - A new handler will be installed with our custom formatter. It will be configured to emit logs at the given level
       (default: DEBUG) or higher, to the specified stream (default: sys.stderr).

    Args:
        level: The logging level to set for the console handler.
        stream: The stream to which the logger will write. Defaults to sys.stderr.
        root: The root logger to modify. Defaults to the system root logger. (Mainly useful in tests.)

    Returns:
        The logging handler that was installed.
    """
    for handler in root.handlers:
        root.removeHandler(handler)
    console_handler = logging.StreamHandler(stream)
    console_handler.setFormatter(NiceFormatter(stream=stream))
    console_handler.setLevel(level)
    root.addHandler(console_handler)
    return console_handler
