"""internall plumbing for passing logging context (dict) to logger instances"""

import dataclasses
import inspect
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from functools import partial, wraps
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Any, TypeVar, get_origin

AnyType = TypeVar("AnyType")

if TYPE_CHECKING:
    SkipLogging = Annotated[AnyType, ...]  # SkipLogging[list[str]] will be treated by type checkers as list[str]
else:

    @dataclasses.dataclass(slots=True)
    class SkipLogging:
        """`@logging_context_params` will ignore parameters annotated with this class."""

        def __class_getitem__(cls, item: Any) -> Any:
            return Annotated[item, SkipLogging()]


_CTX: ContextVar = ContextVar("ctx", default={})


def _params_str(d):
    return ", ".join(f"{k}={v!r}" for k, v in d.items())


def _get_skip_logging_param_names(sig: inspect.Signature):
    """Generates list of parameters names having SkipLogging annotation"""
    for name, param in sig.parameters.items():
        a = param.annotation

        # only consider annotation
        if not a or get_origin(a) is not Annotated:
            continue

        # there can be many annotations for each param
        for m in a.__metadata__:
            if isinstance(m, SkipLogging):
                yield name


def _skip_dict_key(d: dict, keys_to_skip: set):
    return {k: v for k, v in d.items() if k not in keys_to_skip}


def current_context():
    """Returns dictionary of current context set via `with loggin_context(...)` context manager or `@logging_context_params` decorator

    Example:
    current_context()
    >>> {'foo': 'bar', 'a': 2}

    """
    return _CTX.get()


def current_context_repr():
    """Returns repr like "key1=val1, key2=val2" string representation of current_context(), or "" in case context is empty"""
    return _params_str(current_context())


@contextmanager
def logging_context(**kwds):
    """Context manager adding keywords to current loging context. Thread and async safe.

    Example:
    with logging_context(foo="bar", a=2):
        logger.info("hello")
    >>> 2025-06-06 07:15:09,329 - __main__ - INFO - hello (foo='bar', a=2)
    """
    # Get the current context and update it with new keywords
    current_ctx = _CTX.get()
    new_ctx = {**current_ctx, **kwds}
    token = _CTX.set(MappingProxyType(new_ctx))
    try:
        yield _CTX.get()
    except Exception as e:
        # python 3.11+: https://docs.python.org/3.11/tutorial/errors.html#enriching-exceptions-with-notes
        # https://peps.python.org/pep-0678/
        if hasattr(e, "add_note"):
            # __notes__ list[str] is only defined if notes were added, otherwise is not there
            # we only want to add note if there was no note before, otherwise chaining context cause sproblems
            if not getattr(e, "__notes__", None):
                e.add_note(f"Context: {_params_str(current_context())}")

        raise
    finally:
        _CTX.reset(token)


def logging_context_params(func=None, **extra_context):
    """Decorator that automatically adds all the function parameters to current logging context.

    Any passed keyward arguments in will be added to the context. Function parameters take precendnce over the extra keywords in case the names would overlap.

    Parameters annotated with `SkipLogging` will be ignored from being added to the context.

    Example:

    @logging_context_params(foo="bar")
    def do_math(a: int, b: SkipLogging[int]):
        r = pow(a, b)
        logger.info(f"result of {a}**{b} is {r}")
        return r

    >>> 2025-06-06 07:15:09,329 - __main__ - INFO - result of 2**8 is 256 (foo='bar', a=2)

    Note:
    - `a` parameter will be logged, type annotation is optional
    - `b` parameter wont be logged because is it is annotated with `SkipLogging`
    - `foo` parameter will be logged because it is passed as extra context to the decorator

    """

    if func is None:
        p = partial(logging_context_params, **extra_context)
        return p

    # will use function's singature to bind positional params to name of the param
    sig = inspect.signature(func)
    skip_params = set(_get_skip_logging_param_names(sig))

    @wraps(func)
    def wrapper(*args, **kwds):
        # only bind if there are positional args
        # extra context has lower priority than any of the args
        # skip_params is used to filter out parameters that are annotated with SkipLogging

        if args:
            b = sig.bind(*args, **kwds)
            ctx_data = {**extra_context, **_skip_dict_key(b.arguments, skip_params)}
        else:
            ctx_data = {**extra_context, **_skip_dict_key(kwds, skip_params)}

        with logging_context(**ctx_data):
            return func(*args, **kwds)

    return wrapper


class LoggingContextFilter(logging.Filter):
    """Adds curent_context() to the log record."""

    def filter(self, record):
        ctx = current_context()
        record.context = f"({_params_str(ctx)})" if ctx else ""
        return True


class LoggingThreadPoolExecutor(ThreadPoolExecutor):
    """ThreadPoolExecutor drop in replacement that will apply current loging context to all new started threads."""

    def __init__(self, max_workers=None, thread_name_prefix="", initializer=None, initargs=()):
        self.__current_context = current_context()
        self.__wrapped_initializer = initializer

        super().__init__(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
            initializer=self._logging_context_init,
            initargs=initargs,
        )

    def _logging_context_init(self, *args):
        global _CTX
        _CTX.set(self.__current_context)
        if self.__wrapped_initializer:
            self.__wrapped_initializer(*args)
