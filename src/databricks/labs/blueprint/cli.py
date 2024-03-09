"""Baseline CLI for Databricks Labs projects."""

import functools
import inspect
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass

from databricks.sdk import AccountClient, WorkspaceClient

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
        flags = payload["flags"]
        log_level = flags.pop("log_level")
        if log_level == "disabled":
            log_level = "info"
        databricks_logger = logging.getLogger("databricks")
        databricks_logger.setLevel(log_level.upper())
        kwargs = {k.replace("-", "_"): v for k, v in flags.items()}
        try:
            cmd = self._mapping[command]
            if cmd.needs_workspace_client():
                kwargs["w"] = self._workspace_client()
            elif cmd.is_account:
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
