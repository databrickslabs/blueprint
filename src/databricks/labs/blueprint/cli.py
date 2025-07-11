"""Baseline CLI for Databricks Labs projects."""

import functools
import inspect
import json
import logging
import os
import types
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import ParseResult

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.config import with_user_agent_extra

from databricks.labs.blueprint.entrypoint import get_logger, run_main
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.wheels import ProductInfo


@dataclass
class Command:
    name: str
    description: str
    fn: Callable[..., None]
    is_account: bool = False
    is_unauthenticated: bool = False

    def needs_workspace_client(self):
        if self.is_unauthenticated:
            return False
        if self.is_account:
            return False
        return True

    def prompts_argument_name(self) -> str | None:
        sig = inspect.signature(self.fn)
        for param in sig.parameters.values():
            if param.annotation is Prompts:
                return param.name
        return None

    def get_argument_type(self, argument_name: str) -> str | None:
        sig = inspect.signature(self.fn)
        if argument_name not in sig.parameters:
            return None
        annotation = sig.parameters[argument_name].annotation
        if isinstance(annotation, types.UnionType):
            return str(annotation)
        return annotation.__name__


class App:
    def __init__(self, __file: str):
        self._mapping: dict[str, Command] = {}
        self._logger = get_logger(__file)
        self._product_info = ProductInfo(__file)

    def command(self, fn=None, is_account: bool = False, is_unauthenticated: bool = False):
        """Decorator to register a function as a command."""

        def register(func):
            command_name = func.__name__.replace("_", "-")
            if not func.__doc__:
                raise SyntaxError(f"{func.__name__} must have some doc comment")
            self._mapping[command_name] = Command(
                name=command_name,
                description=func.__doc__,
                fn=func,
                is_account=is_account,
                is_unauthenticated=is_unauthenticated,
            )
            return func

        if fn is None:
            return functools.partial(register)
        register(fn)
        return fn

    def _route(self, raw):
        """Route the command. This is the entry point for the CLI."""
        payload = json.loads(raw)
        command = payload["command"]
        if command not in self._mapping:
            msg = f"cannot find command: {command}"
            raise KeyError(msg)
        # user agent is set consistently with the Databricks CLI:
        # see https://github.com/databricks/cli/blob/main/cmd/root/user_agent_command.go#L35-L37
        with_user_agent_extra("cmd", command)
        flags = payload["flags"]
        log_level = flags.pop("log_level")
        if log_level == "disabled":
            log_level = "info"
        databricks_logger = logging.getLogger("databricks")
        databricks_logger.setLevel(log_level.upper())
        kwargs = {k.replace("-", "_"): v for k, v in flags.items() if v != ""}
        cmd = self._mapping[command]
        # modify kwargs to match the type of the argument
        for kwarg in list(kwargs.keys()):
            match cmd.get_argument_type(kwarg):
                case "int":
                    kwargs[kwarg] = int(kwargs[kwarg])
                case "bool":
                    kwargs[kwarg] = kwargs[kwarg].lower() == "true"
                case "float":
                    kwargs[kwarg] = float(kwargs[kwarg])
        try:
            if cmd.needs_workspace_client():
                self._patch_databricks_host()
                kwargs["w"] = self._workspace_client()
            elif cmd.is_account:
                self._patch_databricks_host()
                kwargs["a"] = self._account_client()
            prompts_argument = cmd.prompts_argument_name()
            if prompts_argument:
                kwargs[prompts_argument] = Prompts()
            cmd.fn(**kwargs)
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger = self._logger.getChild(command)
            if log_level.lower() in {"debug", "trace"}:
                logger.error(f"Failed to call {command}", exc_info=err)
            else:
                logger.error(f"{err.__class__.__name__}: {err}")

    @classmethod
    def fix_databricks_host(cls, host: str) -> str:
        """Emulate the way the Go SDK fixes the Databricks host before using it.

        Args:
            host: The host URL to normalize.
        Returns:
            A normalized host URL.
        Raises:
            ValueError: If the host cannot be parsed as a URL.
        """
        parsed = urllib.parse.urlparse(host)

        netloc = parsed.netloc
        # If the netloc is empty, assume the scheme wasn't included.
        if not netloc:
            parsed = urllib.parse.urlparse(f"https://{host}")
        if not parsed.hostname:
            return host

        # Create a new instance to ensure other fields are initialized as empty.
        parsed = ParseResult(scheme=parsed.scheme, netloc=parsed.netloc, path="", params="", query="", fragment="")
        return parsed.geturl()

    def _patch_databricks_host(self) -> None:
        """Patch the DATABRICKS_HOST environment variable if necessary, to work around a host normalization issue.

        The normalization issue arises because the Go SDK normalizes the host differently to the Python SDK, and
        labs CLI integration passes the host from the Go SDK to the Python SDK via DATABRICKS_HOST but (normally)
        without normalizing it first. As such here we emulate the Go SDK's normalization, pending an update
        to the Python SDK to behave the same way.
        """
        host = os.environ.get("DATABRICKS_HOST")
        if not host:
            return

        try:
            fixed_host = self.fix_databricks_host(host)
        except ValueError as e:
            self._logger.debug(f"Failed to parse DATABRICKS_HOST: {host}, will leave as-is.", exc_info=e)
            return

        if fixed_host == host:
            self._logger.debug(f"Leaving DATABRICKS_HOST as-is: {host}")
        else:
            self._logger.warning(f"Working around DATABRICKS_HOST normalization issue: {host} -> {fixed_host}")
            os.environ["DATABRICKS_HOST"] = fixed_host

    def _account_client(self):
        return AccountClient(
            product=self._product_info.product_name(),
            product_version=self._product_info.version(),
        )

    def _workspace_client(self):
        return WorkspaceClient(
            product=self._product_info.product_name(),
            product_version=self._product_info.version(),
        )

    def __call__(self):
        run_main(self._route)
