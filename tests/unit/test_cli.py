import json
import sys
from unittest import mock

from databricks.labs.blueprint.cli import App


def test_commands():
    some = mock.Mock()
    app = App(__file__)

    @app.command(is_unauthenticated=True)
    def foo(name: str):
        """Some comment"""
        some(name)

    with mock.patch.object(
        sys,
        "argv",
        [
            ...,
            json.dumps(
                {
                    "command": "foo",
                    "flags": {
                        "name": "y",
                        "log_level": "disabled",
                    },
                }
            ),
        ],
    ):
        app()

    some.assert_called_with("y")
