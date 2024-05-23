import inspect
import json
import sys
from unittest import mock

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.tui import Prompts

FOO_COMMAND = json.dumps(
    {
        "command": "foo",
        "flags": {
            "name": "y",
            "age": "100",
            "salary": "100.5",
            "address": "",
            "is_customer": "true",
            "log_level": "disabled",
        },
    }
)


def test_commands():
    some = mock.Mock()
    app = App(inspect.getfile(App))

    @app.command(is_unauthenticated=True)
    def foo(name: str, age: int, salary: float, is_customer: bool, address: str = "default"):
        """Some comment"""
        some(name, age, salary, is_customer, address)

    with mock.patch.object(sys, "argv", [..., FOO_COMMAND]):
        app()

    some.assert_called_with("y", 100, 100.5, True, "default")


def test_injects_prompts():
    some = mock.Mock()
    app = App(inspect.getfile(App))

    @app.command(is_unauthenticated=True)
    def foo(name: str, prompts: Prompts):
        """Some comment"""
        assert isinstance(prompts, Prompts)
        some(name)

    with mock.patch.object(sys, "argv", [..., FOO_COMMAND]):
        app()

    some.assert_called_with("y")
