from __future__ import annotations

import io
import logging
from collections.abc import Generator
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
    assert "This is an error message: KeyError: 123" in output
    assert "This is a critical message" in output
