from __future__ import annotations

import asyncio
import datetime as dt
import inspect
import io
import logging
import re
from collections.abc import Generator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from databricks.labs.blueprint.logger import (
    Line,
    NiceFormatter,
    install_logger,
    readlines,
)


class LogCaptureHandler(logging.Handler):
    """Custom logging handler to capture log records."""

    records: list[logging.LogRecord]

    def __init__(self) -> None:
        super().__init__()
        self.records = []

    def emit(self, record: logging.LogRecord) -> None:
        """Capture the log record."""
        self.records.append(record)

    @classmethod
    @contextmanager
    def record_capturing(cls, logger: logging.Logger) -> Generator[LogCaptureHandler, None, None]:
        """Temporarily capture all log records, in addition to existing handling."""
        handler = LogCaptureHandler()
        logger.addHandler(handler)
        try:
            yield handler
        finally:
            logger.removeHandler(handler)


class LoggingSystemFixture:
    """A logging system, independent of the system logger."""

    output_buffer: io.StringIO
    root: logging.RootLogger
    manager: logging.Manager

    def __init__(self) -> None:
        self.output_buffer = io.StringIO()
        self.root = logging.RootLogger(logging.WARNING)
        self.root.addHandler(logging.StreamHandler(self.output_buffer))
        self.manager = logging.Manager(self.root)

    def getLogger(self, name: str) -> logging.Logger:
        """Get a logger that is part of this logging system."""
        return self.manager.getLogger(name)

    def text(self) -> str:
        """Get the formatted text that has been logged by this system so far."""
        return self.output_buffer.getvalue()


@pytest.fixture
def logging_system() -> LoggingSystemFixture:
    """Fixture to provide a logging system independent of the system logger."""
    return LoggingSystemFixture()


def test_install_logger(logging_system) -> None:
    """Test installing the logger.

    This involves verifying that:

     - The existing handlers on the root logger are replaced with a new handler, and it uses the nice formatter.
     - The handler log-level is set, but the root is left as-is.
    """
    root = logging_system.root
    root.setLevel(logging.FATAL)

    # Install the logger and log some things.
    handler = install_logger(logging.INFO, root=root, stream=logging_system.output_buffer)

    # Verify that the root logger was configured as expected.
    assert root.level == logging.FATAL  # remains unchanged
    assert root.handlers == [handler]
    assert handler.level == logging.INFO
    assert isinstance(handler.formatter, NiceFormatter)


def test_installed_logger_logging(logging_system) -> None:
    """Test that logging basics work with the installed logger."""
    root = logging_system.root
    root.setLevel(logging.DEBUG)
    install_logger(stream=logging_system.output_buffer, root=root)

    # Log some messages.
    logger = logging_system.getLogger(__file__)
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message", exc_info=KeyError(123))
    logger.critical("This is a critical message")

    # Verify the messages were logged correctly.
    output = logging_system.text()
    assert "This is a debug message" in output
    assert "This is an info message" in output
    assert "This is a warning message" in output
    assert "This is an error message\nKeyError: 123" in output
    assert "This is a critical message" in output


# Regex that matches the SGR escape sequence to set text attributes (including colors) on terminals/consoles. SGR is:
#    CSI Ps [; ... ; Ps] m
# Where:
#  - CSI: Control Sequence Introducer, ESC + '['
#  - Ps: A number, indicating the attribute to set, 0 to reset. A sequence is allowed, separated by ';'.
#  - m: A literal 'm' character (which indicates the end of SGR sequence).
# Examples:
#   - '\x1b[0m' (reset)
#   - '\x1b[1m' (bold)
#   - '\x1b[1;31m' (bold red)
#   - '\x1b[31;1m' (also bold red)
# These are often referred to as ANSI escape codes.
_SGR_ESCAPE_SEQ = re.compile(r"\x1b\[[\d;]+m")


def _strip_sgr_sequences(text: str) -> str:
    """Strip SGR escape sequences from the text."""
    return _SGR_ESCAPE_SEQ.sub("", text)


# Call signature matches logger.log(), except we return the record.
def create_record(level: int, msg: str, *args, name: str = __name__, **kwargs) -> logging.LogRecord:
    """Create a log record with the given level and message."""
    logger = logging.getLogger(name)

    # Capture existing configuration.
    old_handlers = tuple(logger.handlers)
    old_propagate = logger.propagate
    old_level = logger.level

    try:
        # Ensure the logger actually emits the record to its handler, but doesn't propagate to its parent.
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        for handler in old_handlers:
            logger.removeHandler(handler)

        with LogCaptureHandler.record_capturing(logger) as capture_handler:
            # Create the log record.
            logger.log(level, msg, *args, **kwargs)

            # Return the captured log record.
            records = capture_handler.records
            assert records
            produced_record = capture_handler.records.pop()
            return produced_record
    finally:
        # Restore the logger configuration.
        logger.setLevel(old_level)
        for handler in old_handlers:
            logger.addHandler(handler)
        logger.propagate = old_propagate


def test_formatter_color_if_enabled() -> None:
    """Ensure the formatter includes color codes if colors are enabled."""
    formatter = NiceFormatter()
    formatter.colors = True

    record = create_record(logging.DEBUG, "Arbitrary message.")
    formatted = formatter.format(record)
    stripped = _strip_sgr_sequences(formatted)

    assert stripped != formatted


def test_formatter_skips_colors() -> None:
    """Ensure the formatter does not include color codes if colors are disabled."""
    formatter = NiceFormatter()
    formatter.colors = False

    record = create_record(logging.DEBUG, "Arbitrary message.")
    formatted = formatter.format(record)
    stripped = _strip_sgr_sequences(formatted)

    assert stripped == formatted


@pytest.mark.parametrize("use_colors", (True, False), ids=("with_colors", "without_colors"))
def test_formatter_format_simple_msg(use_colors: bool) -> None:
    """Ensure the formatter formats a simple message correctly."""
    formatter = NiceFormatter()
    formatter.colors = use_colors

    record = create_record(logging.DEBUG, "This is a test message.")
    formatted = formatter.format(record)
    stripped = _strip_sgr_sequences(formatted) if use_colors else formatted

    # H:M:S LEVEL [logger_name] message
    assert stripped.endswith(" This is a test message.")


@pytest.mark.parametrize("use_colors", (True, False), ids=("with_colors", "without_colors"))
def test_formatter_format_msg_with_args(use_colors: bool) -> None:
    """Ensure the formatter correctly formats a message with arguments that need to be interpolated."""
    formatter = NiceFormatter()
    formatter.colors = use_colors

    record = create_record(logging.DEBUG, "This is a %s message with %d arguments.", "test", 2)
    formatted = formatter.format(record)
    stripped = _strip_sgr_sequences(formatted) if use_colors else formatted

    # H:M:S LEVEL [logger_name] message
    assert stripped.endswith(" This is a test message with 2 arguments.")


@pytest.mark.parametrize("use_colors", (True, False), ids=["with_colors", "without_colors"])
def test_formatter_timestamp(use_colors: bool) -> None:
    """Ensure the formatter starts with the timestamp."""
    formatter = NiceFormatter()
    formatter.colors = use_colors

    record = create_record(logging.DEBUG, "Whatever")

    formatted = formatter.format(record)

    # Deliberately naive: we want the local time rather than UTC.
    record_timestamp = dt.datetime.fromtimestamp(record.created, tz=None)
    stripped = _strip_sgr_sequences(formatted) if use_colors else formatted

    # H:M:S LEVEL [logger_name] message
    formatted_timestamp = record_timestamp.strftime("%H:%M:%S")
    assert stripped.startswith(f"{formatted_timestamp} ")


@pytest.mark.parametrize(
    "level",
    (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL),
    ids=lambda level: logging.getLevelName(level),
)
@pytest.mark.parametrize("use_colors", (True, False), ids=("with_colors", "without_colors"))
def test_formatter_format_log_level(level: int, use_colors: bool) -> None:
    """Ensure the formatter formats a simple message correctly."""
    formatter = NiceFormatter()
    formatter.colors = use_colors

    # Create a log record with the specified level.
    record = create_record(level, "Whatever")
    formatted = formatter.format(record)
    stripped = _strip_sgr_sequences(formatted) if use_colors else formatted

    # H:M:S LEVEL [logger_name] message
    assert f" {logging.getLevelName(level)} " in stripped


# Logger names, and their abbreviated forms.
_logger_names = {
    "foo": "foo",
    "foo.bar": "foo.bar",
    "woo.foo.bar": "w.foo.bar",
    # Canonical example.
    "databricks.labs.ucx.foo.bar": "d.l.u.foo.bar",
    # Corner-case.
    "....foo.bar": "....foo.bar",
}


@pytest.mark.parametrize(("logger_name", "formatted_name"), tuple(_logger_names.items()))
def test_formatter_format_colorized_logger_name_abbreviated(logger_name: str, formatted_name: str) -> None:
    """Ensure the logger name is abbreviated in colorized formatting."""
    formatter = NiceFormatter()
    formatter.colors = True

    # Create a log record with the specified level.
    record = create_record(logging.DEBUG, "Whatever", name=logger_name)
    formatted = formatter.format(record)
    stripped = _strip_sgr_sequences(formatted)

    # H:M:S LEVEL [logger_name] message
    assert f" [{formatted_name}] " in stripped


@pytest.mark.parametrize("logger_name", tuple(_logger_names.keys()))
def test_formatter_format_non_colorized_logger_name_as_is(logger_name: str) -> None:
    """Ensure the logger name is left as-is for non-colorized formatting."""
    formatter = NiceFormatter()
    formatter.colors = False

    # Create a log record with the specified level.
    record = create_record(logging.DEBUG, "Whatever", name=logger_name)
    formatted = formatter.format(record)

    # H:M:S LEVEL [logger_name] message
    assert f" [{logger_name}] " in formatted


def test_formatter_format_colorized_thread_name() -> None:
    """The colorized formatter includes the thread name if non-main."""
    formatter = NiceFormatter()
    formatter.colors = True

    # Create a log record with the specified level.
    main_record = create_record(logging.DEBUG, "Record from main thread")
    assert main_record.threadName == "MainThread"
    assert " [MainThread] " not in _strip_sgr_sequences(formatter.format(main_record))

    # Create a log record on a different thread.
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="temporary-test-worker") as executor:
        future = executor.submit(create_record, logging.DEBUG, "Record from worker thread")
        thread_record = future.result()
    assert thread_record.threadName and thread_record.threadName.startswith("temporary-test-worker")
    # H:M:S LEVEL [logger_name][thread_name] message
    assert f"][{thread_record.threadName}] " in _strip_sgr_sequences(formatter.format(thread_record))


@pytest.mark.parametrize("use_colors", (True, False), ids=("with_colors", "without_colors"))
def test_formatter_format_exception(use_colors: bool) -> None:
    """The colorized formatter includes the thread name if non-main."""
    formatter = NiceFormatter()
    formatter.colors = use_colors

    # Create a log record that includes attached exception information.
    try:
        exception_message = "Test exception."
        currentframe = inspect.currentframe()
        assert currentframe
        exception_line = inspect.getframeinfo(currentframe).lineno + 1
        raise RuntimeError(exception_message)
    except RuntimeError:
        record = create_record(logging.DEBUG, "Record with exception", exc_info=True)
    formatted = formatter.format(record)
    stripped = _strip_sgr_sequences(formatted) if use_colors else formatted

    # H:M:S LEVEL [logger_name] message\n
    # Traceback (most recent call last):\n
    #   File "PATH", line X, in <module>\n
    #     source_of_line
    # exc_type: exc_message
    lines = stripped.splitlines()
    msg, *traceback, exception = lines
    assert msg.endswith(" Record with exception")
    assert traceback == [
        "Traceback (most recent call last):",
        f'  File "{__file__}", line {exception_line}, in test_formatter_format_exception',
        "    raise RuntimeError(exception_message)",
    ]
    assert exception == "RuntimeError: Test exception."


class MockStreamReader(asyncio.StreamReader):
    """Mock asyncio.StreamReader that returns pre-configured chunks."""

    _remaining_data: Sequence[bytes]

    def __init__(self, data_chunks: Sequence[bytes]) -> None:
        super().__init__()
        # Chunks represent data that could be returned on successive reads, mimicking the nature of non-blocking I/O
        # where partial data may be returned. The chunk boundaries represent the splits where partial data is returned.
        self._remaining_data = data_chunks

    async def read(self, n: int = -1) -> bytes:
        match n:
            case -1:
                # Read all remaining data.
                data = b"".join(self._remaining_data)
                self._remaining_data = []
            case 0:
                # Empty read.
                data = b""
            case max_read if max_read > 0:
                # Read up to n, but only from the first chunk.
                match self._remaining_data:
                    case []:
                        data = b""
                    case [head, *tail]:
                        if len(head) <= max_read:
                            data = head
                            self._remaining_data = tail
                        else:
                            data = head[:max_read]
                            self._remaining_data = [head[max_read:], *tail]
            case _:
                raise ValueError(f"Unsupported read size: {n}")
        return data

    async def readline(self) -> bytes:
        raise NotImplementedError("This is a mock; not implemented.")

    async def readexactly(self, n: int) -> bytes:
        raise NotImplementedError("This is a mock; not implemented.")

    async def readuntil(self, separator: bytes = b"\n") -> bytes:
        raise NotImplementedError("This is a mock; not implemented.")

    def at_eof(self) -> bool:
        return not self._remaining_data

    @classmethod
    def line(cls, text_dsl: str) -> Line:
        """Create a Line instance from the given text, setting the flags based on trailing markers.

        The markers are:
         - If the text ends with '+' it indicates truncation.
         - If the text ends with '!' it indicates this is the final line.

        The markers are mutually exclusive and not part of the actual text.

        Args:
            text_dsl: The text with optional trailing markers.
        Returns:
            The Line instance representing the supplied text and flags.
        """
        if text_dsl.endswith("+"):
            return Line(text=text_dsl[:-1], is_truncated=True)
        elif text_dsl.endswith("!"):
            return Line(text=text_dsl[:-1], is_final=True)
        else:
            return Line(text=text_dsl, is_truncated=False, is_final=False)

    @classmethod
    async def assert_readlines_with_chunks_yields_lines(
        cls,
        data_chunks: Sequence[bytes],
        expected_lines: Sequence[str | Line],
        *,
        limit: int = 128,
    ) -> None:
        stream = cls(data_chunks)
        lines = [line async for line in readlines(stream=stream, limit=limit)]
        normalized_lines = [x if isinstance(x, Line) else cls.line(x) for x in expected_lines]
        assert normalized_lines == lines


async def test_readlines_normal_lines() -> None:
    """Verify the simple case of each line fitting within the limit: one line per read."""
    data_chunks = (b"first line\n", b"second line\n", b"third line\n")
    expected_lines = ("first line", "second line", "third line")
    await MockStreamReader.assert_readlines_with_chunks_yields_lines(data_chunks, expected_lines)


async def test_readlines_whitespace_handling() -> None:
    """Verify that whitespace (excluding newlines) and empty lines are preserved."""
    data_chunks = (b"  first line  \n", b"\tsecond\rline\t\r\n", b" \t \r\n", b"\n", b"last\tproper\tline\n", b" \t \r")
    expected_lines = ("  first line  ", "\tsecond\rline\t", " \t ", "", "last\tproper\tline", " \t \r!")
    await MockStreamReader.assert_readlines_with_chunks_yields_lines(data_chunks, expected_lines)


async def test_readlines_small_reads() -> None:
    """Verify that lines split over multiple small sub-limit reads are correctly reassembled."""
    data_chunks = (b"first ", b"line\nsecond", b" line\r", b"\nlas", b"t line")
    expected_lines = ("first line", "second line", "last line!")
    await MockStreamReader.assert_readlines_with_chunks_yields_lines(data_chunks, expected_lines)


@pytest.mark.parametrize(
    ("data_chunks", "expected_messages"),
    (
        # Note: limit for all examples is 10.
        # Single line split over 2 reads.
        ((b"1234567", b"89\n"), ("123456789",)),
        # Single read, exactly on the limit.
        ((b"123456789\n",), ("123456789",)),
        # Single read, exactly on the minimum limit to trigger premature flush.
        ((b"1234567890",), ("1234567890+",)),
        # Maximum line length.
        ((b"123456789", b"123456789\n"), ("1234567891+", "23456789")),
        # Multiple lines in one read, with existing data from the previous read.
        ((b"1", b"12\n45\n78\n0", b"12\n"), ("112", "45", "78", "012")),
        # A very long line, with some existing data in the buffer, and leaving some remainder.
        (
            (b"12", b"3456789012" b"3456789012" b"3456789012" b"34567890\n1234"),
            ("1234567890+", "1234567890+", "1234567890+", "1234567890+", "", "1234!"),
        ),
        # A \r\n newline split across reads.
        ((b"1234\r", b"\nabcd\n"), ("1234", "abcd")),
        # A \r\n split exactly on the limit.
        ((b"123456789\r" b"\nabcd\n",), ("123456789+", "", "abcd")),
    ),
)
async def test_readlines_line_exceeds_limit(data_chunks: Sequence[bytes], expected_messages: Sequence[str]) -> None:
    """Verify that line buffering and splitting is handled, including if a line is (much!) longer than the limit."""
    await MockStreamReader.assert_readlines_with_chunks_yields_lines(data_chunks, expected_messages, limit=10)


async def test_readlines_incomplete_line_at_eof() -> None:
    """Verify that an incomplete line at EOF is logged."""
    data_chunks = (b"normal_line\n", b"incomplete_line\r")
    expected_messages = ("normal_line", "incomplete_line\r!")
    await MockStreamReader.assert_readlines_with_chunks_yields_lines(data_chunks, expected_messages)


async def test_readlines_invalid_utf8() -> None:
    """Test invalid UTF-8 sequences are replaced with replacement character."""
    data_chunks = (
        # A line with invalid UTF-8 bytes in it.
        b"bad[\xc0\xc0]utf8\n",
        # An unterminated UTF-8 sequence at the end of the file.
        b"incomplete\xc3"
    )
    expected_messages = ("bad[\ufffd\ufffd]utf8", "incomplete\ufffd!")
    await MockStreamReader.assert_readlines_with_chunks_yields_lines(data_chunks, expected_messages, limit=16)


async def test_readlines_split_utf8() -> None:
    """Test that UTF-8 sequence split across limit-based chunks is handled properly."""
    # A long line, that will be split across the utf-8 sequence: the character will be deferred until the line.
    data_chunks = ("123456789abcd\U0001f596efgh\n".encode("utf-8"),)
    expected_messages = ("123456789abcd+", "\U0001f596efgh")
    await MockStreamReader.assert_readlines_with_chunks_yields_lines(data_chunks, expected_messages, limit=16)


async def test_readlines_empty_stream() -> None:
    """Verify that an empty stream yields no lines."""
    await MockStreamReader.assert_readlines_with_chunks_yields_lines(data_chunks=(), expected_lines=())


async def test_readlines_invalid_limit() -> None:
    """Verify that an invalid limit raises ValueError."""
    stream = MockStreamReader(data_chunks=())
    with pytest.raises(
        ValueError, match=re.escape("Limit must be at least 2 to allow for meaningful line reading, but got 1.")
    ):
        async for _ in readlines(stream=stream, limit=1):
            pass


async def test_default_line_representation() -> None:
    """Verify that the default Line representation is as expected."""
    # Note: just for convenience/display purposes.
    assert str(Line(text="Here is a line")) == "Here is a line"
    assert str(Line(text="Truncated line", is_truncated=True)) == "Truncated line[\u2026]"
    assert str(Line(text="Last incomplete line", is_final=True)) == "Last incomplete line[no eol]"
