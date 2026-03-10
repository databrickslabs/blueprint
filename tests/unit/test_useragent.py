import contextlib
import inspect
import json
import os
import sys
import types
import typing
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from unittest import mock

from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.__about__ import __version__
from databricks.labs.blueprint.cli import App

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


@contextlib.contextmanager
def http_fixture_server(handler: typing.Callable[[BaseHTTPRequestHandler], None]) -> Generator[str]:

    class _Handler(BaseHTTPRequestHandler):
        def __init__(self, *args) -> None:
            self._do_ALL = types.MethodType(handler, self)
            super().__init__(*args)

        def __getattr__(self, item: str) -> typing.Callable[[BaseHTTPRequestHandler], None]:
            if not item.startswith("do_"):
                raise AttributeError(f"method {item} not found")
            return self._do_ALL

    srv = HTTPServer(("localhost", 0), _Handler)
    t = Thread(target=srv.serve_forever)
    try:
        t.daemon = True
        t.start()
        yield "http://{0}:{1}".format(*srv.server_address)
    finally:
        srv.shutdown()


def test_user_agent_is_propagated() -> None:
    user_agent: dict[str, list[str]] = {}
    app = App(inspect.getfile(App))

    def inner(h: BaseHTTPRequestHandler):
        for pair in h.headers["User-Agent"].split(" "):
            if "/" not in pair:
                continue
            k, v = pair.split("/")
            user_agent.setdefault(k, []).append(v)
        h.send_response(200)
        h.send_header("Content-Type", "application/json")
        h.end_headers()
        h.wfile.write(b"{}")
        h.wfile.flush()

    @app.command
    def foo(w: WorkspaceClient, **_):
        """Some comment"""
        w.current_user.me()

    with http_fixture_server(inner) as host:
        with mock.patch.dict(os.environ, {"DATABRICKS_HOST": host, "DATABRICKS_TOKEN": "_"}, clear=True):
            with mock.patch.object(sys, "argv", [..., FOO_COMMAND]):
                app()

    assert "blueprint" in user_agent
    assert "cmd" in user_agent
    assert __version__ in user_agent["blueprint"]
    assert "foo" in user_agent["cmd"]
