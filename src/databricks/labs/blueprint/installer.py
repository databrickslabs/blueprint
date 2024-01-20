import dataclasses
import json
import logging
import threading
from typing import Any


from databricks.labs.blueprint.installation import Installation

logger = logging.getLogger(__name__)

Resources = dict[str, str]
Json = dict[str, Any]


@dataclasses.dataclass
class RawState:
    __file__ = "state.json"
    __version__ = 1

    resources: dict[str, Resources]


class InstallState:
    """Manages ~/.{product}/state.json file on WorkspaceFS to track installations"""

    _state: RawState | None = None

    def __init__(self, installation: Installation):
        self._lock = threading.Lock()

    def __getattr__(self, item: str) -> Resources:
        with self._lock:
            if not self._state:
                self._state = self._load()
            if item not in self._state["resources"]:
                self._state["resources"][item] = {}
            return self._state["resources"][item]

    def save(self) -> None:
        """Saves remote state"""
        state: dict = {}
        if self._state:
            state = self._state.copy()  # type: ignore[assignment]
        state["$version"] = self._config_version
        state_dump = json.dumps(state, indent=2).encode("utf8")
        self._overwrite("state.json", state_dump)
