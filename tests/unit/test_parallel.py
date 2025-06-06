import logging
from functools import partial

from databricks.sdk.core import DatabricksError

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.blueprint.logger import logging_context_params


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
        assert getattr(e, '__notes__', None) is None


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

    for e in errors:
        assert e.__notes__ is not None
