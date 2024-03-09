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
            "log_level": "disabled",
        },
    }
)


def test_commands():
    some = mock.Mock()
    app = App(inspect.getfile(App))

    @app.command(is_unauthenticated=True)
    def foo(name: str):
        """Some comment"""
        some(name)

    with mock.patch.object(sys, "argv", [..., FOO_COMMAND]):
        app()

    some.assert_called_with("y")


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
