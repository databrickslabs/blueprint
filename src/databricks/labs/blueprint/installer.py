import abc
import dataclasses
import enum
import json
import logging
import threading
import types
import typing
from functools import partial
from json import JSONDecodeError
from typing import Any, Callable, TypedDict, io

import databricks.sdk.core
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.mixins import workspace
from databricks.sdk.service.workspace import ImportFormat

logger = logging.getLogger(__name__)

Resources = dict[str, str]
Json = dict[str, Any]


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

    def save(self) -> None:
        """Saves remote state"""
        state: dict = {}
        if self._state:
            state = self._state.copy()  # type: ignore[assignment]
        state["$version"] = self._config_version
        state_dump = json.dumps(state, indent=2).encode("utf8")
        self._overwrite("state.json", state_dump)

    T = typing.TypeVar("T")

    def load_typed_file(self, type_ref: typing.Type[T], *, filename: str = None) -> T:
        # TODO: load with type_ref, convert JSON/YAML into a dataclass instance, discover format migrations from methods
        # TODO: detect databricks config and allow using it as part of dataclass instance
        # TODO: MockInstallState to get JSON/YAML created/loaded as dict-per-filename
        if not filename and hasattr(type_ref, "__file__"):
            filename = getattr(type_ref, "__file__")
        elif not filename:
            filename = f"{type_ref.__name__}.json"
        expected_version = None
        if hasattr(type_ref, "__version__"):
            expected_version = getattr(type_ref, "__version__")
        as_dict = self._load_content(filename)
        if expected_version:
            actual_version = as_dict.pop("$version", 1)
            while actual_version < expected_version:
                migrate = getattr(type_ref, f"v{actual_version}_migrate", None)
                if not migrate:
                    break
                as_dict = migrate(as_dict)
                actual_version = as_dict.pop("$version", 1)
            if actual_version != expected_version:
                raise IllegalState(f"expected state $version={expected_version}, got={actual_version}")
        raise NotImplementedError

    def save_typed_file(self, inst: T, *, filename: str = None):
        # TODO: consider: save_configuration([Foo(1,2),Foo(3,4)], filename='tables.csv')
        if not inst:
            raise TypeError("missing value")
        type_ref = type(inst)
        if not filename and hasattr(inst, "__file__"):
            filename = getattr(inst, "__file__")
        elif not filename:
            filename = f"{type_ref.__name__}.json"
        version = None
        if hasattr(inst, "__version__"):
            version = getattr(inst, "__version__")
        as_dict, _ = self._marshal(type_ref, [], inst)
        if version:
            as_dict["$version"] = version
        self._overwrite_content(filename, as_dict)
        return f"{self.install_folder()}/{filename}"

    def load_csv(self, type_ref: typing.Type[T]) -> list[T]:
        # TODO: load/save arrays in CSV format
        # TODO: MockInstallState to get CSV file created/loaded as slice-of-dataclasses
        raise NotImplementedError

    def save_csv(self, records: list[T], *, filename: str | None = None) -> list[T]:
        # TODO: load/save arrays in CSV format - perhaps do the
        raise NotImplementedError

    def list_files(self) -> list[workspace.ObjectInfo]:
        # TODO: list files under install folder
        raise NotImplementedError

    # TODO: add from_dict to databricks config (or make it temporary hack in unmarshaller)

    def upload_to_dbfs(self) -> str:
        # TODO: use this in Wheels to upload/download random files into correct prefix in WSFS/DBFS
        with self._local_wheel.open("rb") as f:
            self._ws.dbfs.mkdirs(self._remote_dir_name)
            logger.info(f"Uploading wheel to dbfs:{self._remote_wheel}")
            self._ws.dbfs.upload(self._remote_wheel, f, overwrite=True)
        return self._remote_wheel

    def upload_to_wsfs(self, filename: str, raw: bytes) -> str:
        # TODO: use in wheels
        return self._overwrite(filename, raw)

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

    def _overwrite_content(self, filename: str, as_dict: Json):
        converters = {"json": partial(json.dumps, indent=2), "yml": self._dump_yaml}
        extension = filename.split(".")[-1]
        if extension not in converters:
            raise KeyError(f"Unknown extension: {extension}")
        self._overwrite(filename, converters[extension](as_dict))

    def _overwrite(self, filename: str, raw: bytes):
        with self._lock:
            dst = f"{self.install_folder()}/{filename}"
            attempt = partial(self._ws.workspace.upload, dst, raw, format=ImportFormat.AUTO, overwrite=True)
            try:
                attempt()
            except NotFound:
                self._ws.workspace.mkdirs(self.install_folder())
                attempt()
            return dst

    def _load_content(self, filename: str) -> Json:
        with self._lock:
            converters = {"json": json.load, "yml": self._load_yaml}
            extension = filename.split(".")[-1]
            if extension not in converters:
                raise KeyError(f"Unknown extension: {extension}")
            try:
                with self._ws.workspace.download(f"{self.install_folder()}/{filename}") as f:
                    return converters[extension](f)
            except JSONDecodeError:
                return {}
            except NotFound:
                return {}

    def _marshal(self, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        if dataclasses.is_dataclass(type_ref):
            if inst is None:
                return None, False
            as_dict = {}
            for field, hint in typing.get_type_hints(type_ref).items():
                raw = getattr(inst, field)
                value, ok = self._marshal(hint, [*path, field], raw)
                if not ok:
                    raise TypeError(self._explain_why(hint, [*path, field], raw))
                if not value:
                    continue
                as_dict[field] = value
            return as_dict, True
        if isinstance(type_ref, types.GenericAlias):
            type_args = typing.get_args(type_ref)
            if not type_args:
                raise TypeError(f"Missing type arguments: {type_args}")
            values = []
            hint = type_args[0]
            if not inst:
                return None, False
            for i, v in enumerate(inst):
                value, ok = self._marshal(hint, [*path, f"{i}"], v)
                if not ok:
                    raise TypeError(self._explain_why(hint, [*path, f"{i}"], v))
                values.append(value)
            return values, True
        if isinstance(type_ref, (types.UnionType, typing._UnionGenericAlias)):
            combo = []
            for variant in typing.get_args(type_ref):
                value, ok = self._marshal(variant, [*path, f"(as {variant})"], inst)
                if ok:
                    return value, True
                combo.append(self._explain_why(variant, [*path, f"(as {variant})"], inst))
            raise TypeError(f'{".".join(path)}: union: {" or ".join(combo)}')
        if isinstance(type_ref, typing._GenericAlias):
            if not inst:
                return None, False
            return inst, isinstance(inst, type_ref.__origin__)
        if isinstance(inst, databricks.sdk.core.Config):
            return inst.as_dict(), True
        if type_ref == list:
            values = []
            for i, v in enumerate(inst):
                hint = type(v)
                value, ok = self._marshal(hint, [*path, f"{i}"], v)
                if not ok:
                    raise TypeError(self._explain_why(hint, [*path, f"{i}"], v))
                values.append(value)
            return values, True
        if isinstance(type_ref, enum.EnumMeta):
            if not inst:
                return None, False
            return inst.value, True
        if type_ref == types.NoneType:
            return inst, inst is None
        if type_ref in (int, bool, float, str):
            return inst, True
        raise TypeError(f'{".".join(path)}: unknown: {inst}')

    @staticmethod
    def _explain_why(type_ref: type, path: list[str], raw: Any) -> str:
        if raw is None:
            raw = "value is missing"
        return f'{".".join(path)}: not a {type_ref.__name__}: {raw}'

    @staticmethod
    def _dump_yaml(raw: Json) -> bytes:
        try:
            return yaml.dump(raw).encode("utf8")
        except ImportError:
            raise SyntaxError("PyYAML is not installed. Fix: pip install databricks-labs-blueprint[yaml]")

    @staticmethod
    def _load_yaml(raw: typing.BinaryIO) -> Json:
        try:
            try:
                return yaml.safe_load(raw)
            except yaml.YAMLError as err:
                raise JSONDecodeError(str(err), "<yaml>", 0)
        except ImportError:
            raise SyntaxError("PyYAML is not installed. Fix: pip install databricks-labs-blueprint[yaml]")


class MockInstallState(InstallState):
    """Install state testing toolbelt

    register with PyTest:

        pytest.register_assert_rewrite('databricks.labs.blueprint.installer')
    """

    def __init__(self, overwrites: dict[str, Json] = None):
        if not overwrites:
            overwrites = {}
        self._overwrites = overwrites

    def install_folder(self) -> str:
        return "~/mock/"

    def _overwrite_content(self, filename: str, as_dict: Json):
        self._overwrites[filename] = as_dict

    def _load_content(self, filename: str) -> Json:
        return self._overwrites[filename]

    def assert_file_written(self, filename: str, expected: Any):
        assert filename in self._overwrites, f"{filename} had no writes"
        actual = self._overwrites[filename]
        assert expected == actual, f"{filename} content missmatch"
