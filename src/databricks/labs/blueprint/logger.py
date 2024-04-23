"""A nice formatter for logging. It uses colors and bold text if the console supports it."""

import logging
import sys


class NiceFormatter(logging.Formatter):
    """A nice formatter for logging. It uses colors and bold text if the console supports it."""

    BOLD = "\033[1m"
    RESET = "\033[0m"
    GREEN = "\033[32m"
    BLACK = "\033[30m"
    CYAN = "\033[36m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    MAGENTA = "\033[35m"
    GRAY = "\033[90m"

    def __init__(self, *, probe_tty: bool = False) -> None:
        """Create a new instance of the formatter. If probe_tty is True, then the formatter will
        attempt to detect if the console supports colors. If probe_tty is False, colors will be
        enabled by default."""
        super().__init__(fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s", datefmt="%H:%M")
        self._levels = {
            logging.NOTSET: self._bold("TRACE"),
            logging.DEBUG: self._bold(f"{self.CYAN}DEBUG"),
            logging.INFO: self._bold(f"{self.GREEN} INFO"),
            logging.WARNING: self._bold(f"{self.YELLOW} WARN"),
            logging.ERROR: self._bold(f"{self.RED}ERROR"),
            logging.CRITICAL: self._bold(f"{self.MAGENTA}FATAL"),
        }
        # show colors in runtime, github actions, and while debugging
        self.colors = sys.stdout.isatty() if probe_tty else True

    def _bold(self, text):
        """Return text in bold."""
        return f"{self.BOLD}{text}{self.RESET}"

    def format(self, record: logging.LogRecord):  # noqa: A003
        """Format the log record. If colors are enabled, use them."""
        if not self.colors:
            return super().format(record)
        timestamp = self.formatTime(record, datefmt="%H:%M:%S")
        level = self._levels[record.levelno]
        # databricks.labs.ucx.foo.bar -> d.l.u.foo.bar
        module_split = record.name.split(".")
        last_two_modules = len(module_split) - 2
        name = ".".join(part if i >= last_two_modules else part[0] for i, part in enumerate(module_split))
        msg = record.msg
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            msg += ": " + record.exc_text
        if record.stack_info:
            if msg[-1:] != "\n":
                msg += "\n"
            msg += self.formatStack(record.stack_info)

        color_marker = self.GRAY
        if record.levelno in (logging.INFO, logging.WARNING):
            color_marker = self.BOLD
        elif record.levelno in (logging.ERROR, logging.FATAL):
            color_marker = self.RED + self.BOLD
        thread_name = ""
        if record.threadName != "MainThread":
            thread_name = f"[{record.threadName}]"
        return f"{self.GRAY}{timestamp}{self.RESET} {level} {color_marker}[{name}]{thread_name} {msg}{self.RESET}"


def install_logger(level="DEBUG"):
    """Install a console logger with a nice formatter."""
    for handler in logging.root.handlers:
        logging.root.removeHandler(handler)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(NiceFormatter())
    console_handler.setLevel(level)
    logging.root.addHandler(console_handler)
    return console_handler
