"""A nice formatter for logging. It uses colors and bold text if the console supports it."""

import asyncio
import codecs
import logging
import sys
from collections.abc import AsyncGenerator
from dataclasses import dataclass
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


@dataclass(frozen=True, kw_only=True)
class Line:
    """Represent a single line of (potentially truncated) log output."""

    text: str
    """The text of the line."""

    is_truncated: bool = False
    """Whether the line was truncated, with the remainder pending."""

    is_final: bool = False
    """Whether this is the final (incomplete) line in the stream."""

    def __str__(self) -> str:
        """Return the text of the line, appending an ellipsis if it was truncated."""
        # This is for display purposes only.
        suffix = ""
        if self.is_truncated:
            suffix += "[\u2026]"
        if self.is_final:
            suffix += "[no eol]"
        return f"{self.text}{suffix}"


async def readlines(*, stream: asyncio.StreamReader, limit: int = 8192) -> AsyncGenerator[Line]:
    """Read lines from the given stream, yielding them as they arrive.

    The lines will be yielded in real-time as they arrive, once the newline character is seen. Semi-universal
    newlines are supported: "\n" and "\r\n" both terminate lines (but not "\r" alone).

    On EOF any pending line will be yielded, even if it is incomplete (i.e. does not end with a newline).

    The stream being read is treated as UTF-8, with invalid byte sequences replaced with the Unicode replacement
    character.

    Long lines will be split into chunks with a maximum length.

    Args:
          stream: The stream to mirror as logger output.
          limit: The maximum number of bytes for a line before it is yielded anyway even though a newline has not been
            encountered. Longer lines will therefore be split into chunks (as they arrive) no larger than this limit.
            Default: 8192.
    """
    if limit < 2:
        msg = f"Limit must be at least 2 to allow for meaningful line reading, but got {limit}."
        raise ValueError(msg)
    # Maximum size of pending buffer is the limit argument.
    pending_buffer = bytearray()
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    # Implementation note: the buffer management here is a bit intricate because we want to ensure that:
    #  - We don't copy data around more than necessary. (Where possible we use memoryview() to avoid copies.)
    #  - We never want to have more than 'limit' bytes pending at any time; this is to avoid unbounded memory usage.
    #  - Temporary memory usage is kept to a minimum, again to avoid excessive memory usage. The various dels are
    #    present to ensure that potentially large data chunks are released as soon as possible.

    # Loop reading whatever data is available as it arrives, being careful to never have more than `limit` bytes pending.
    while chunk := await stream.read(limit - len(pending_buffer)):
        # Process the chunk we've read, which may contain multiple lines, line by line.
        line_from = 0
        while -1 != (eol := chunk.find(b"\n", line_from)):
            # Step 1: Figure out the slice corresponding to this line, handling any pending data from the last read.
            line_chunk = memoryview(chunk)[line_from:eol]
            line_bytes: bytearray | bytes
            if pending_buffer:
                pending_buffer.extend(line_chunk)
                line_bytes = pending_buffer
            else:
                line_bytes = bytes(line_chunk)
            del line_chunk

            # Step 2: Decode the line and yield it.
            line = Line(text=decoder.decode(line_bytes).rstrip("\r"))
            del line_bytes
            yield line
            del line

            # Step 3: Set up for handling the next line of this chunk.
            pending_buffer.clear()
            line_from = eol + 1

        # Anything remaining in this chunk is pending data for the next read, but some corner cases need to be handled:
        #  - This chunk may not have any newlines, and we may already have pending data from the previous chunk we read.
        #  - We may be at the limit (including any pending data from earlier reads) and need to yield an incomplete
        #    line.
        if remaining := memoryview(chunk)[line_from:]:
            pending_buffer.extend(remaining)
            if len(pending_buffer) >= limit:
                # Line too long, yield what we have and reset.
                # (As a special case, postpone handling a trailing \r: it could be part of a \r\n newline sequence.)
                yield_through = (limit - 1) if pending_buffer.endswith(b"\r") else limit
                yield_now = pending_buffer[:yield_through]
                line = Line(text=decoder.decode(yield_now), is_truncated=True)
                del yield_now
                yield line
                del line, pending_buffer[:yield_through]
        del remaining
    if pending_buffer:
        # Here we've hit EOF but have an incomplete line pending. We need to yield it.
        line = Line(text=decoder.decode(pending_buffer, final=True), is_final=True)
        yield line
