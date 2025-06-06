import logging

from databricks.labs.blueprint._logging_context import LoggingThreadPoolExecutor
from databricks.labs.blueprint.logger import (
    SkipLogging,
    current_context,
    logging_context,
    logging_context_params,
)


def test_nested_logger_context():
    logger = logging.getLogger(__name__)

    ctx0 = current_context()
    logger.info("before entering context")

    with logging_context(user="Alice", action="read") as ctx1:
        logger.info("inside of first context")
        assert ctx1 == {"user": "Alice", "action": "read"}

        with logging_context(action="write") as ctx2:
            logger.info("inner context")
            assert ctx2 == {"user": "Alice", "action": "write"}

        logger.info("still inside first")
        assert current_context() == ctx1

    logger.info("after exiting context")
    assert current_context() == ctx0

    with logging_context(user="Bob", action="write") as ctx2:
        logger.info("inside of second context")
        assert ctx2 == {"user": "Bob", "action": "write"}


def test_exception_with_notest_flat():
    logger = logging.getLogger(__name__)

    try:
        with logging_context(
            user="Alice",
            action="read",
        ):
            logger.info("inside of first context")
            with logging_context(top_secret="47"):
                1 / 0
    except Exception as e:
        logger.exception(f"Exception! {e}")
        assert e.__notes__ == ["Context: user='Alice', action='read', top_secret='47'"]
        assert str(e) == "division by zero"


def test_exception_with_notest_nested():
    logger = logging.getLogger(__name__)
    logger.info("before entering context")

    try:
        with logging_context(file="foo.txt"):
            with logging_context(
                user="Alice",
                action="read",
            ):
                logger.info("inside of first context")
                1 / 0
    except Exception as e:
        logger.exception(f"Exception! {e}")
        logger.error(f"Error! {e}")
        assert e.__notes__ == ["Context: file='foo.txt', user='Alice', action='read'"]
        assert str(e) == "division by zero"


def test_logging_function_params_empty_deco_call():
    logger = logging.getLogger(__name__)

    @logging_context_params()
    def do_math_verbose_test(a: int, b):
        r = pow(a, b)
        logger.info(f"result of {a}**{b} is {r}")
        assert current_context() == {"a": a, "b": b}
        return r

    assert current_context() == {}
    do_math_verbose_test(2, b=8)
    assert current_context() == {}
    do_math_verbose_test(2, b=7)
    assert current_context() == {}


def test_logging_function_params_no_call():
    logger = logging.getLogger(__name__)

    @logging_context_params
    def do_math_verbose_test(a: int, b):
        r = pow(a, b)
        logger.info(f"result of {a}**{b} is {r}")
        assert current_context() == {"a": a, "b": b}
        return r

    assert current_context() == {}
    do_math_verbose_test(2, b=8)
    assert current_context() == {}
    do_math_verbose_test(2, b=7)
    assert current_context() == {}


def test_logging_function_skip_loggingl():
    logger = logging.getLogger(__name__)

    @logging_context_params
    def do_math_verbose_test(a: SkipLogging[float], b):
        r = pow(a, b)
        logger.info(f"result of {a}**{b} is {r}")
        assert current_context() == {"b": b}
        return r

    assert current_context() == {}
    do_math_verbose_test(2, b=8)
    assert current_context() == {}
    do_math_verbose_test(2, b=7)
    assert current_context() == {}


def test_logging_function_params_shadow_deco_call():
    logger = logging.getLogger(__name__)

    @logging_context_params(a="bar")
    def do_math_verbose_test(a: int, b):
        r = pow(a, b)
        logger.info(f"result of {a}**{b} is {r}")
        assert current_context() == {"a": a, "b": b}
        return r

    assert current_context() == {}
    do_math_verbose_test(2, b=8)
    assert current_context() == {}
    do_math_verbose_test(2, b=7)
    assert current_context() == {}


def test_logging_function_params_non_shadow_deco_call():
    logger = logging.getLogger(__name__)

    @logging_context_params(foo="bar")
    def do_math_verbose_test(a: int, b):
        r = pow(a, b)
        logger.info(f"result of {a}**{b} is {r}")
        assert current_context() == {"foo": "bar", "a": a, "b": b}
        return r

    assert current_context() == {}
    do_math_verbose_test(2, b=8)
    assert current_context() == {}
    do_math_verbose_test(2, b=7)
    assert current_context() == {}


def test_logging_function_params_multiple_contexts():
    logger = logging.getLogger(__name__)

    @logging_context_params(foo="bar")
    def do_math_verbose_test(a: int, b):
        r = pow(a, b)
        logger.info(f"result of {a}**{b} is {r}")
        assert current_context() == {"foo": "bar", "a": a, "b": b, "x": "6"}
        return r

    with logging_context(x="6"):
        do_math_verbose_test(2, b=8)


def test_logging_thread_pool():
    logger = logging.getLogger(__name__)

    @logging_context_params(foo="bar")
    def do_math_verbose(a, b: int):
        r = pow(a, b)
        logger.info(f"result of {a}**{b} is {r}")
        assert current_context() == {"foo": "bar", "a": a, "b": b, "user": "Alice"}
        return r

    def do_math_verbose_without_context(a, b: int):
        r = pow(a, b)
        logger.info(f"result of {a}**{b} is {r}")
        assert current_context() == {"foo": "bar" if a == 2 else "zar", "a": a, "b": b, "user": "Alice"}
        return r

    with logging_context(user="Alice"):
        futures = []
        with LoggingThreadPoolExecutor(max_workers=1) as executor:
            futures.append(executor.submit(do_math_verbose, 2, 2))
            futures.append(executor.submit(do_math_verbose, 2, 6))
            futures.append(executor.submit(logging_context_params(foo="zar")(do_math_verbose_without_context), 3, 8))
            futures.append(executor.submit(logging_context_params(foo="zar")(do_math_verbose_without_context), 3, 12))

            for f in futures:
                f.result()
