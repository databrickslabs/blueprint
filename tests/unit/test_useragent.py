import contextlib
import functools
import inspect
import json
import os
import sys
import typing
from http.server import BaseHTTPRequestHandler
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
def http_fixture_server(handler: typing.Callable[[BaseHTTPRequestHandler], None]):
    from http.server import HTTPServer
    from threading import Thread

    class _handler(BaseHTTPRequestHandler):
        def __init__(self, handler: typing.Callable[[BaseHTTPRequestHandler], None], *args):
            self._handler = handler
            super().__init__(*args)

        def __getattr__(self, item):
            if "do_" != item[0:3]:
                raise AttributeError(f"method {item} not found")
            return functools.partial(self._handler, self)

    handler_factory = functools.partial(_handler, handler)
    srv = HTTPServer(("localhost", 0), handler_factory)
    t = Thread(target=srv.serve_forever)
    try:
        t.daemon = True
        t.start()
        yield "http://{0}:{1}".format(*srv.server_address)
    finally:
        srv.shutdown()


def test_user_agent_is_propagated():
    user_agent = {}
    app = App(inspect.getfile(App))

    def inner(h: BaseHTTPRequestHandler):
        for pair in h.headers["User-Agent"].split(" "):
            if "/" not in pair:
                continue
            k, v = pair.split("/")
            user_agent[k] = v
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
    assert user_agent["blueprint"] == __version__
    assert user_agent["cmd"] == "foo"
