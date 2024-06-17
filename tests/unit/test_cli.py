import inspect
import json
import sys
from unittest import mock
from unittest.mock import create_autospec

from databricks.sdk import AccountClient

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
            "optional_arg": "optional",
        },
    }
)


def test_commands():
    some = mock.Mock()
    app = App(inspect.getfile(App))

    @app.command(is_unauthenticated=True)
    def foo(
        name: str,
        age: int,
        salary: float,
        is_customer: bool,
        address: str = "default",
        optional_arg: str | None = None,
    ):
        """Some comment"""
        some(name, age, salary, is_customer, address, optional_arg)

    with mock.patch.object(sys, "argv", [..., FOO_COMMAND]):
        app()

    some.assert_called_with("y", 100, 100.5, True, "default", "optional")


def test_injects_prompts():
    some = mock.Mock()
    app = App(inspect.getfile(App))

    @app.command(is_unauthenticated=True)
    def foo(
        name: str,
        age: int,
        salary: float,
        is_customer: bool,
        prompts: Prompts,
        address: str = "default",
        optional_arg: str | None = None,
    ):
        """Some comment"""
        assert isinstance(prompts, Prompts)
        some(name, age, salary, is_customer, address, optional_arg)

    with mock.patch.object(sys, "argv", [..., FOO_COMMAND]):
        app()

    some.assert_called_with("y", 100, 100.5, True, "default", "optional")


def test_collection_commands(mocker):
    some = mock.Mock()
    app = App(inspect.getfile(App))
    acc_client = create_autospec(AccountClient)
    mocker.patch("databricks.sdk.AccountClient.__new__", mock.Mock(return_value=acc_client))

    @app.command(is_unauthenticated=False, is_collection=True)
    def foo(
        name: str,
        age: int,
        salary: float,
        is_customer: bool,
        a: AccountClient,
        collection_workspace_id: int = 1234,
        address: str = "default",
        optional_arg: str | None = None,
    ):
        """Some comment"""
        some(name, age, salary, is_customer, collection_workspace_id, address, optional_arg, a)

    with mock.patch.object(sys, "argv", [..., FOO_COMMAND]):
        app()

    some.assert_called_with("y", 100, 100.5, True, 1234, "default", "optional", acc_client)
