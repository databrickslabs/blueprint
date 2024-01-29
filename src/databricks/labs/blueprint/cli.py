import functools
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass

from databricks.sdk import AccountClient, WorkspaceClient

from databricks.labs.blueprint.entrypoint import get_logger, run_main
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


class App:
    def __init__(self, __file: str):
        self._mapping: dict[str, Command] = {}
        self._logger = get_logger(__file)
        self._product_info = ProductInfo(__file)

    def command(self, fn=None, is_account: bool = False, is_unauthenticated: bool = False):
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
            product_name = self._product_info.product_name()
            product_version = self._product_info.version()
            if self._mapping[command].needs_workspace_client():
                kwargs["w"] = WorkspaceClient(product=product_name, product_version=product_version)
            elif self._mapping[command].is_account:
                kwargs["a"] = AccountClient(product=product_name, product_version=product_version)
            self._mapping[command].fn(**kwargs)
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger = self._logger.getChild(command)
            if log_level.lower() in {"debug", "trace"}:
                logger.error(f"Failed to call {command}", exc_info=err)
            else:
                logger.error(f"{err.__class__.__name__}: {err}")

    def __call__(self):
        run_main(self._route)
