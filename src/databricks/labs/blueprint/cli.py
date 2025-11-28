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


# The types of arguments that can be passed to commands.
_CommandArg = int | str | bool | float | WorkspaceClient | AccountClient | Prompts


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

    def _log_level(self, raw: str) -> int:
        """Convert the log-level provided by the Databricks CLI into a logging level supported by Python."""
        # Log levels at the time of writing:
        # https://github.com/databricks/cli/blob/071b584105d42034a05fd8c3f8a81fb2d9760f54/libs/log/levels.go#L6
        log_level: int
        match raw.upper():
            case "DISABLED":
                # Default from the Databricks CLI when nothing has been explicitly set by the user.
                log_level = logging.INFO
            case "TRACE":
                log_level = logging.DEBUG
            case other:
                log_level = logging.getLevelName(other)
                if not isinstance(log_level, int):
                    self._logger.warning(f"Assuming INFO-level logging due to unrecognized log-level: {raw}")
                    log_level = logging.INFO
        return log_level

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
        log_level = self._log_level(flags.pop("log_level"))
        databricks_logger = logging.getLogger("databricks")
        databricks_logger.setLevel(log_level)
        cmd = self._mapping[command]
        kwargs = self._build_args(cmd, flags)
        try:
            cmd.fn(**kwargs)
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger = self._logger.getChild(command)
            if log_level == logging.DEBUG:
                logger.error(f"Failed to call {command}", exc_info=err)
            else:
                logger.error(f"{err.__class__.__name__}: {err}")

    def _build_args(self, cmd: Command, flags: dict[str, str]) -> dict[str, _CommandArg]:
        kwargs: dict[str, _CommandArg] = {k.replace("-", "_"): v for k, v in flags.items() if v != ""}
        # modify kwargs to match the type of the argument
        for kwarg in list(kwargs.keys()):
            value = kwargs[kwarg]
            if not isinstance(value, str):
                continue
            match cmd.get_argument_type(kwarg):
                case "int":
                    kwargs[kwarg] = int(value)
                case "bool":
                    kwargs[kwarg] = value.lower() == "true"
                case "float":
                    kwargs[kwarg] = float(value)
        if cmd.needs_workspace_client():
            self._patch_databricks_host()
            kwargs["w"] = self._workspace_client()
        elif cmd.is_account:
            self._patch_databricks_host()
            kwargs["a"] = self._account_client()
        prompts_argument = cmd.prompts_argument_name()
        if prompts_argument:
            kwargs[prompts_argument] = Prompts()
        return kwargs

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

    def _account_client(self) -> AccountClient:
        return AccountClient(
            product=self._product_info.product_name(),
            product_version=self._product_info.version(),
        )

    def _workspace_client(self) -> WorkspaceClient:
        return WorkspaceClient(
            product=self._product_info.product_name(),
            product_version=self._product_info.version(),
        )

    def __call__(self):
        run_main(self._route)
