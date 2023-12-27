import json
import logging
import threading
from json import JSONDecodeError
from typing import TypedDict

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

logger = logging.getLogger(__name__)

Resources = dict[str, str]


class RawState(TypedDict):
    resources: dict[str, Resources]


class IllegalState(ValueError):
    pass


class InstallState:
    """Manages ~/.{product}/state.json file on WorkspaceFS to track installations"""

    _state: RawState | None = None

    def __init__(
        self, ws: WorkspaceClient, product: str, config_version: int = 1, *, install_folder: str | None = None
    ):
        self._ws = ws
        self._product = product
        self._install_folder = install_folder
        self._config_version = config_version
        self._lock = threading.Lock()

    def product(self) -> str:
        return self._product

    def install_folder(self) -> str:
        if self._install_folder:
            return self._install_folder
        me = self._ws.current_user.me()
        self._install_folder = f"/Users/{me.user_name}/.{self._product}"
        return self._install_folder

    def __getattr__(self, item: str) -> Resources:
        with self._lock:
            if not self._state:
                self._state = self._load()
            if item not in self._state["resources"]:
                self._state["resources"][item] = {}
            return self._state["resources"][item]

    def _state_file(self) -> str:
        return f"{self.install_folder()}/state.json"

    def _load(self) -> RawState:
        """Loads remote state"""
        default_state: RawState = {"resources": {}}
        try:
            raw = json.load(self._ws.workspace.download(self._state_file()))
            version = raw.pop("$version", None)
            if version != self._config_version:
                msg = f"expected state $version={self._config_version}, got={version}"
                raise IllegalState(msg)
            return raw
        except NotFound:
            return default_state
        except JSONDecodeError:
            logger.warning(f"JSON state file corrupt: {self._state_file}")
            return default_state

    def save(self) -> None:
        """Saves remote state"""
        with self._lock:
            state: dict = {}
            if self._state:
                state = self._state.copy()  # type: ignore[assignment]
            state["$version"] = self._config_version
            state_dump = json.dumps(state, indent=2).encode("utf8")
            self._ws.workspace.upload(
                self._state_file(),
                state_dump,  # type: ignore[arg-type]
                format=ImportFormat.AUTO,
                overwrite=True,
            )
