import inspect
import json
import logging
import os
import sys
from unittest import mock

import pytest
from databricks.sdk import AccountClient, WorkspaceClient

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


@pytest.mark.parametrize(
    ("original", "expected"),
    (
        # Taken from the Go SDK tests
        ("https://dbc-XXXXXXXX-YYYY.cloud.databricks.com/", "https://dbc-XXXXXXXX-YYYY.cloud.databricks.com"),
        ("https://adb-123.4.azuredatabricks.net/", "https://adb-123.4.azuredatabricks.net"),
        ("https://123.4.gcp.databricks.com/", "https://123.4.gcp.databricks.com"),
        ("https://accounts.cloud.databricks.com", "https://accounts.cloud.databricks.com"),
        ("https://accounts-dod.cloud.databricks.us", "https://accounts-dod.cloud.databricks.us"),
        ("https://accounts-dod.cloud.databricks.us/", "https://accounts-dod.cloud.databricks.us"),
        ("https://my-workspace.cloud.databricks.us", "https://my-workspace.cloud.databricks.us"),
        ("https://my-workspace.cloud.databricks.us/", "https://my-workspace.cloud.databricks.us"),
        # Handle hosts that aren't URLs.
        ("adb-123.4.azuredatabricks.net", "https://adb-123.4.azuredatabricks.net"),
        ("adb-123.4.azuredatabricks.net:443", "https://adb-123.4.azuredatabricks.net:443"),
        # The login URL, which is accepted by the Go SDK.
        ("https://adb-123.4.azuredatabricks.net/login.html?o=123", "https://adb-123.4.azuredatabricks.net"),
        # Some errors that we just leave alone; the SDK will handle them.
        ("https://:443", "https://:443"),
    ),
)
def test_databricks_host_workaround(original: str, expected: str) -> None:
    """Test that the normalization of the Databricks host works as expected."""
    fixed = App.fix_databricks_host(original)

    assert fixed == expected


@pytest.mark.parametrize(
    "erroneous",
    (
        "https://[foobar",
        "[foobar",
    ),
)
def test_databricks_host_workaround_error_handling(erroneous: str) -> None:
    """Test that we raise the expected error if the host is malformed."""
    with pytest.raises(ValueError):
        App.fix_databricks_host(erroneous)


class _TestApp(App):
    def __init__(self) -> None:
        super().__init__(inspect.getfile(App))

    def _account_client(self):
        return mock.create_autospec(AccountClient, instance=True)

    def _workspace_client(self):
        return mock.create_autospec(WorkspaceClient, instance=True)

    def invoke(self, *argv):
        self._route(*argv)


def test_databricks_workspace_host_patch(monkeypatch, caplog) -> None:
    """Test that the patching of the DATABRICKS_HOST environment variable works for the CLI when a workspace client is used."""

    test_app = _TestApp()
    captured_databricks_host: list[str | None] = []

    @test_app.command
    def capture_workspace_databricks_host(w: WorkspaceClient) -> None:
        """A command that needs a workspace client but does nothing."""
        assert w is not None
        captured_databricks_host.append(os.environ.get("DATABRICKS_HOST"))

    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.4.azuredatabricks.net/login.html?o=123")

    cli_payload = {
        "command": "capture-workspace-databricks-host",
        "flags": {"log_level": "debug"},
    }
    with caplog.at_level("DEBUG"):
        test_app.invoke(*[json.dumps(cli_payload)])

    assert captured_databricks_host == ["https://adb-123.4.azuredatabricks.net"]
    warning_messages = [record.message for record in caplog.records if record.levelno == logging.WARNING]
    expected_warning = "Working around DATABRICKS_HOST normalization issue: https://adb-123.4.azuredatabricks.net/login.html?o=123 -> https://adb-123.4.azuredatabricks.net"
    assert expected_warning in warning_messages


def test_databricks_account_host_patch(monkeypatch, caplog) -> None:
    """Test that the patching of the DATABRICKS_HOST environment variable works for the CLI when an account client is used."""

    test_app = _TestApp()
    captured_databricks_host: list[str | None] = []

    @test_app.command(is_account=True)
    def capture_account_databricks_host(a: AccountClient) -> None:
        """A command that needs an account client but does nothing."""
        assert a is not None
        captured_databricks_host.append(os.environ.get("DATABRICKS_HOST"))

    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.4.azuredatabricks.net/login.html?o=123")

    cli_payload = {
        "command": "capture-account-databricks-host",
        "flags": {"log_level": "debug"},
    }
    with caplog.at_level("DEBUG"):
        test_app.invoke(*[json.dumps(cli_payload)])

    assert captured_databricks_host == ["https://adb-123.4.azuredatabricks.net"]
    warning_messages = [record.message for record in caplog.records if record.levelno == logging.WARNING]
    expected_warning = "Working around DATABRICKS_HOST normalization issue: https://adb-123.4.azuredatabricks.net/login.html?o=123 -> https://adb-123.4.azuredatabricks.net"
    assert expected_warning in warning_messages
