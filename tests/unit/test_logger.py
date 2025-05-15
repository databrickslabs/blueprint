from __future__ import annotations

import datetime as dt
import inspect
import io
import logging
import re
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from databricks.labs.blueprint.logger import NiceFormatter, install_logger


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

    # Can't mark combinations of parameters for xfail, so we simulate it here.
    expected_failure = use_colors and level in (logging.WARNING, logging.CRITICAL)
    try:
        # H:M:S LEVEL [logger_name] message
        assert f" {logging.getLevelName(level)} " in stripped
        if expected_failure:
            msg = (
                f"Unexpected success: colorized log-level for {logging.getLevelName(level)} is thought to be incorrect."
            )
            pytest.fail(msg)
    except AssertionError:
        if not expected_failure:
            raise
        pytest.xfail(f"Colorized log-level formatting for {logging.getLevelName(level)} is known to be incorrect.")


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
    # Can't easily mark this as known to sometimes faili, so we simulate it here.
    expected_failure = ".." in logger_name
    try:
        formatted = formatter.format(record)
        if expected_failure:
            pytest.fail("Unexpected success: colorized logger name abbreviation is though to fail when .. is present.")
    except IndexError:
        if not expected_failure:
            raise
        pytest.xfail("Colorized logger name abbreviation is known to fail when .. is present.")
        return
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
