import logging
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.retries import retried

from databricks.labs.blueprint.installation import IllegalState, Installation

logger = logging.getLogger(__name__)

Json = dict[str, Any]


@dataclass
class RawState:
    __file__ = "state.json"
    __version__ = 1

    resources: dict[str, dict[str, str]] = field(default_factory=dict)


class StateError(IllegalState):
    pass


class InstallState:
    """Manages ~/.{product}/state.json file on WorkspaceFS to track installations"""

    _state: RawState | None = None

    def __init__(self, ws: WorkspaceClient, product: str, *, install_folder: str | None = None):
        self._installation = Installation(ws, product, install_folder=install_folder)
        self._lock = threading.Lock()

    def install_folder(self):
        return self._installation.install_folder()

    @retried(on=[StateError], timeout=timedelta(seconds=5))
    def __getattr__(self, item: str) -> dict[str, str]:
        with self._lock:
            if not self._state:
                self._state = self._installation.load(RawState)
        if not self._state:
            raise StateError("Failed to load raw state")
        if item not in self._state.resources:
            self._state.resources[item] = {}
        return self._state.resources[item]

    def save(self) -> None:
        """Saves remote state"""
        with self._lock:
            self._installation.save(self._state)
