import logging
import os
import sys
from functools import partial
from unittest.mock import MagicMock, patch

from databricks.sdk.core import DatabricksError

from databricks.labs.blueprint.logger import logging_context_params
from databricks.labs.blueprint.parallel import Threads

# only python 3.11 supports notes in exceptions, hence test only on these version...
SUPPORTS_NOTES = (sys.version_info[0], sys.version_info[1]) >= (3, 11)


def _predictable_messages(caplog):
    res = []
    for msg in caplog.messages:
        if "rps" in msg:
            continue
        msg = msg.split(". Took ")[0]  # noqa: PLW2901
        res.append(msg)
    return sorted(res)


def test_gather_with_failed_task(caplog):
    caplog.set_level(logging.INFO)

    def works():
        return True

    def fails():
        msg = "failed"
        raise DatabricksError(msg)

    tasks = [works, fails, works, fails, works, fails, works, fails]
    results, errors = Threads.gather("testing", tasks)

    assert [True, True, True, True] == results
    assert 4 == len(errors)
    assert [
        "More than half 'testing' tasks failed: 50% results available (4/8)",
        "testing task failed: failed",
        "testing task failed: failed",
        "testing task failed: failed",
        "testing task failed: failed",
    ] == _predictable_messages(caplog)


def test_gather_with_failed_task_no_message(caplog):
    caplog.set_level(logging.INFO)

    def works():
        return True

    def not_really_but_fine():
        logging.info("did something, but returned None")

    def fails():
        msg = "failed"
        raise OSError(1, msg)

    tasks = [works, not_really_but_fine, works, fails, works, works]
    results, errors = Threads.gather("testing", tasks)

    assert [True, True, True, True] == results
    assert 1 == len(errors)
    assert [
        "Some 'testing' tasks failed: 67% results available (4/6)",
        "did something, but returned None",
        "testing task failed: [Errno 1] failed",
    ] == _predictable_messages(caplog)


def test_all_none(caplog):
    caplog.set_level(logging.INFO)

    def not_really_but_fine():
        logging.info("did something, but returned None")

    tasks = [not_really_but_fine, not_really_but_fine, not_really_but_fine, not_really_but_fine]
    results, errors = Threads.gather("testing", tasks)

    assert [] == results
    assert [] == errors
    assert [
        "Finished 'testing' tasks: 0% results available (0/4)",
        "did something, but returned None",
        "did something, but returned None",
        "did something, but returned None",
        "did something, but returned None",
    ] == _predictable_messages(caplog)


def test_all_failed(caplog):
    caplog.set_level(logging.INFO)

    def fails():
        msg = "failed"
        raise DatabricksError(msg)

    tasks = [fails, fails, fails, fails]
    results, errors = Threads.gather("testing", tasks)

    assert [] == results
    assert 4 == len(errors)
    assert [
        "All 'testing' tasks failed!!!",
        "testing task failed: failed",
        "testing task failed: failed",
        "testing task failed: failed",
        "testing task failed: failed",
    ] == _predictable_messages(caplog)


def test_all_works(caplog):
    caplog.set_level(logging.INFO)

    def works():
        return True

    tasks = [works, works, works, works]
    results, errors = Threads.gather("testing", tasks)

    assert [True, True, True, True] == results
    assert 0 == len(errors)
    assert ["Finished 'testing' tasks: 100% results available (4/4)"] == _predictable_messages(caplog)


def test_odd_partial_failed(caplog):
    caplog.set_level(logging.INFO)

    def fails_on_odd(n=1, dummy=None):
        if isinstance(n, str):
            raise RuntimeError("strings are not supported!")

        if n % 2:
            msg = "failed"
            raise DatabricksError(msg)

    tasks = [
        partial(fails_on_odd, n=1),
        partial(fails_on_odd, 1, dummy="6"),
        partial(fails_on_odd),
        partial(fails_on_odd, n="aaa"),
    ]

    results, errors = Threads.gather("testing", tasks)

    assert [] == results
    assert 4 == len(errors)
    assert [
        "All 'testing' tasks failed!!!",
        "testing task failed: failed",
        "testing(1, dummy='6') task failed: failed",
        "testing(n='aaa') task failed: strings are not supported!",
        "testing(n=1) task failed: failed",
    ] == _predictable_messages(caplog)

    # not context, no notes
    for e in errors:
        assert getattr(e, "__notes__", None) is None


def test_odd_partial_failed_with_context(caplog):
    caplog.set_level(logging.INFO)

    # it will push context information into notes into Execeptions
    @logging_context_params
    def fails_on_odd(n=1, dummy=None):
        if isinstance(n, str):
            raise RuntimeError("strings are not supported!")

        if n % 2:
            msg = "failed"
            raise DatabricksError(msg)

    tasks = [
        partial(fails_on_odd, n=1),
        partial(fails_on_odd, 1, dummy="6"),
        partial(fails_on_odd),
        partial(fails_on_odd, n="aaa"),
    ]

    results, errors = Threads.gather("testing", tasks)

    assert [] == results
    assert 4 == len(errors)
    assert [
        "All 'testing' tasks failed!!!",
        "testing task failed: failed",
        "testing(1, dummy='6') task failed: failed",
        "testing(n='aaa') task failed: strings are not supported!",
        "testing(n=1) task failed: failed",
    ] == _predictable_messages(caplog)

    if SUPPORTS_NOTES:
        for e in errors:
            assert e.__notes__ is not None


def test_cpu_count() -> None:
    """Verify a CPU count is available."""
    assert 0 < Threads.available_cpu_count(), "CPU count should be greater than 0"


def test_cpu_count_source_process_cpu_count() -> None:
    """Verify the os.process_cpu_count() source is used to determine CPU count, if available."""

    # Some mocks for the various methods that can be used to determine CPU count.
    mock_process_cpu_count = MagicMock(return_value=13)
    mock_sched_getaffinity = MagicMock()
    mock_cpu_count = MagicMock()

    # First priority is os.process_cpu_count() if that's available.
    with (
        patch("os.process_cpu_count", mock_process_cpu_count, create=True),
        patch("os.sched_getaffinity", mock_sched_getaffinity, create=True),
        patch("os.cpu_count", mock_cpu_count),
    ):
        assert Threads.available_cpu_count() == 13, "Should use os.process_cpu_count() if available"
    assert mock_process_cpu_count.called, "os.process_cpu_count() should be called"
    assert (
        not mock_sched_getaffinity.called
    ), "os.sched_getaffinity() should not be called if os.process_cpu_count() is available"
    assert not mock_cpu_count.called, "os.cpu_count() should not be called if os.process_cpu_count() is available"


def test_cpu_count_source_sched_getaffinity(monkeypatch) -> None:
    """Verify that os.sched_getaffinity() is used to determine CPU count, if necessary."""

    # Some mocks for the various methods that can be used to determine CPU count.
    mock_sched_getaffinity = MagicMock(return_value=set(range(1003)))
    mock_cpu_count = MagicMock()

    # After os.process_cpu_count(), the next source to use if available is os.sched_getaffinity().
    monkeypatch.delattr(os, "process_cpu_count", raising=False)
    monkeypatch.setattr(os, "sched_getaffinity", mock_sched_getaffinity, raising=False)
    monkeypatch.setattr(os, "cpu_count", mock_cpu_count)

    assert Threads.available_cpu_count() == 1003, "Should use os.sched_getaffinity() if available"
    assert mock_sched_getaffinity.called, "os.sched_getaffinity() should be called"
    assert not mock_cpu_count.called, "os.cpu_count() should not be called if os.process_cpu_count() is available"


def test_cpu_count_source_cpu_count(monkeypatch) -> None:
    """Verify that os.cpu_count() is used to determine CPU count, if necessary."""

    # A mock for the os.cpu_count() method.
    mock_cpu_count = MagicMock(return_value=735)

    # After os.process_cpu_count(), and os.sched_getaffinity(), the next source to use if available is os.cpu_count().
    monkeypatch.delattr(os, "process_cpu_count", raising=False)
    monkeypatch.delattr(os, "sched_getaffinity", raising=False)
    monkeypatch.setattr(os, "cpu_count", mock_cpu_count)

    assert Threads.available_cpu_count() == 735, "Should use os.cpu_count() to determine the CPU count"
    assert mock_cpu_count.called, "os.cpu_count() should have been called to determine the CPU count"


def test_cpu_count_default(monkeypatch) -> None:
    """Verify that if there is no way to determine the CPU count, we default to 1."""

    # A mock for the os.cpu_count() method.
    mock_cpu_count = MagicMock(return_value=None)

    # Ensure that cpu_count() is the only method available to determine CPU count, and it returns None.
    # After os.process_cpu_count(), and os.sched_getaffinity(), the next source to use if available is os.cpu_count().
    monkeypatch.delattr(os, "process_cpu_count", raising=False)
    monkeypatch.delattr(os, "sched_getaffinity", raising=False)
    monkeypatch.setattr(os, "cpu_count", mock_cpu_count)

    assert Threads.available_cpu_count() == 1, "Should use os.cpu_count() to determine the CPU count"
    assert mock_cpu_count.called, "os.cpu_count() should have been called to determine the CPU count"
