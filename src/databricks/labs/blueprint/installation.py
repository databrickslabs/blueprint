import csv
import dataclasses
import enum
import functools
import io
import json
import logging
import os.path
import re
import threading
import types
from collections.abc import Callable, Collection
from functools import partial
from json import JSONDecodeError
from pathlib import Path
from typing import Any, BinaryIO, TypeVar, get_args, get_type_hints

import databricks.sdk.core
import yaml  # pylint: disable=wrong-import-order
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.mixins import workspace
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.blueprint.parallel import Threads

logger = logging.getLogger(__name__)

Json = dict[str, Any]

__all__ = ["Installation", "IllegalState", "NotInstalled", "SerdeError"]


class IllegalState(ValueError):
    pass


class NotInstalled(NotFound):
    pass


class SerdeError(TypeError):
    pass


class Installation:
    """The `Installation` class is used to manage the `~/.{product}` folder on WorkspaceFS to track typed files.
    It provides methods for serializing and deserializing objects of a specific type, as well as managing the storage
    location for those objects. The class includes methods for loading and saving objects, uploading and downloading
    files, and managing the installation folder.

    The `Installation` class can be helpful for unit testing by allowing you to mock the file system and control
    the behavior of the `load` and `save` methods. You can create a `MockInstallation` object and use it to override
    the default installation folder and the contents of the files in that folder. This allows you to test the behavior
    of your code in different scenarios, such as when a file is not found or when the contents of a file do not match
    the expected format."""

    T = TypeVar("T")
    _PRIMITIVES = (int, bool, float, str)

    def __init__(self, ws: WorkspaceClient, product: str, *, install_folder: str | None = None):
        self._ws = ws
        self._product = product
        self._install_folder = install_folder
        self._lock = threading.Lock()

    @classmethod
    def current(cls, ws: WorkspaceClient, product: str, *, assume_user: bool = False) -> "Installation":
        """Returns the Installation object for the given product in the current workspace.

        If the installation is not found, a `NotInstalled` error is raised. If `assume_user` argument is True, the method
        will assume that the installation is in the user's home directory and return it if found. If False, the method
        will only return an installation that is in the `/Applications` directory."""
        user_folder = cls._user_home_installation(ws, product)
        applications_folder = f"/Applications/{product}"
        folders = [user_folder, applications_folder]
        for candidate in folders:
            try:
                ws.workspace.get_status(candidate)
                return cls(ws, product, install_folder=candidate)
            except NotFound:
                logger.debug(f"{product} is not installed at {candidate}")
                continue
        if assume_user:
            return cls(ws, product, install_folder=user_folder)
        raise NotInstalled(f"Application not installed: {product}")

    @classmethod
    def existing(cls, ws: WorkspaceClient, product: str) -> Collection["Installation"]:
        """Returns a collection of all existing installations for the given product in the current workspace.

        This method searches for installations in the root /Applications directory and home directories of all users
        in the workspace."""

        def check_folder(install_folder: str) -> Installation | None:
            try:
                ws.workspace.get_status(install_folder)
                return cls(ws, product, install_folder=install_folder)
            except NotFound:
                return None

        tasks = [functools.partial(check_folder, f"/Applications/{product}")]
        for user in ws.users.list(attributes="user_name"):
            user_folder = f"/Users/{user.user_name}/.{product}"
            tasks.append(functools.partial(check_folder, user_folder))
        return Threads.strict(f"finding {product} installations", tasks)

    @classmethod
    def load_local(cls, type_ref: type[T], file: Path) -> T:
        with file.open("rb") as f:
            as_dict = cls._convert_content(file.name, f)
            return cls._unmarshal_type(as_dict, file.name, type_ref)

    def product(self) -> str:
        return self._product

    def install_folder(self) -> str:
        """The `install_folder` method returns the path to the installation folder on WorkspaceFS.
        The installation folder is used to store typed files that are managed by the `Installation` class.

        If an `install_folder` argument is provided to the constructor of the `Installation` class, it will be used
        as the installation folder. Otherwise, the installation folder will be determined based on the current user's
        username. Specifically, the installation folder will be `/Users/{user_name}/.{product}`, where `{user_name}`
        is the username of the current user and `{product}` is the name of the product associated with the installation.

        Here is an example of how you can use the `install_folder` method:

        ```
        # Create an Installation object for the "blueprint" product
        install = Installation(WorkspaceClient(), "blueprint")

        # Print the path to the installation folder
        print(install.install_folder())
        # Output: /Users/{user_name}/.blueprint
        ```

        In this example, the `Installation` object is created for the "blueprint" product. The `install_folder` method
        is then called to print the path to the installation folder. The output will be `/Users/{user_name}/.blueprint`,
        where `{user_name}` is the username of the current user.

        You can also provide an `install_folder` argument to the constructor to specify a custom installation folder.
        Here is an example of how you can do this:

        ```
        # Create an Installation object for the "blueprint" product with a custom installation folder
        install = Installation(WorkspaceClient(), "blueprint", install_folder="/my/custom/folder")

        # Print the path to the installation folder
        print(install.install_folder())
        # Output: /my/custom/folder
        ```

        In this example, the `Installation` object is created for the "blueprint" product with a custom installation
        folder of `/my/custom/folder`. The `install_folder` method is then called to print the path to the installation
        folder. The output will be `/my/custom/folder`."""
        if self._install_folder is not None:
            return self._install_folder
        self._install_folder = self._user_home_installation(self._ws, self.product())
        return self._install_folder

    def username(self) -> str:
        return os.path.basename(os.path.dirname(self.install_folder()))

    def load(self, type_ref: type[T], *, filename: str | None = None) -> T:
        """The `load` method loads an object of type `type_ref` from a file on WorkspaceFS. If no `filename` is
        provided, the `__file__` attribute of `type_ref` will be used as the filename.

        If the object has a `__version__` attribute, the method will check that the version of the object in the file
        matches the expected version. If the versions do not match, the method will attempt to migrate the object to
        the expected version using a method named `v{actual_version}_migrate` on the `type_ref` class. If the migration
        is successful, the method will return the migrated object. If the migration is not successful, the method will
        raise an `IllegalState` exception."""
        filename = self._get_filename(filename, type_ref)
        logger.debug(f"Loading {type_ref.__name__} from {filename}")
        as_dict = self._load_content(filename)
        return self._unmarshal_type(as_dict, filename, type_ref)

    def save(self, inst: T, *, filename: str | None = None):
        """The `save` method saves a dataclass object of type `T` to a file on WorkspaceFS.
        If no `filename` is provided, the name of the `type_ref` class will be used as the filename.
        If the object has a `__version__` attribute, the method will add a `$version` field to the serialized object
        with the value of the `__version__` attribute.

        Here is an example of how you can use the `save` method:

        ```
        # Create an Installation object for the "blueprint" product
        install = Installation(WorkspaceClient(), "blueprint")

        # Save a dataclass object of type MyClass to a file
        @dataclasses.dataclass
        class MyClass:
            field1: str
            field2: str

        obj = MyClass(field1='value1', field2='value2')
        install.save(obj, filename='myfile.json')

        # Verify that the object was saved correctly
        loaded_obj = install.load(MyClass, filename='myfile.json')
        assert loaded_obj == obj
        ```

        In this example, the `Installation` object is created for the "blueprint" product. A dataclass object of type
        `MyClass` is then created and saved to a file using the `save` method. The object is then loaded from the file
        using the `load` method and compared to the original object to verify that it was saved correctly."""
        if not inst:
            raise SerdeError("missing value")
        type_ref = self._get_type_ref(inst)
        filename = self._get_filename(filename, type_ref)
        version = None
        if hasattr(inst, "__version__"):
            version = getattr(inst, "__version__")
        as_dict, _ = self._marshal(type_ref, [], inst)
        if version:
            as_dict["$version"] = version
        self._overwrite_content(filename, as_dict, type_ref)
        return f"{self.install_folder()}/{filename}"

    def upload(self, filename: str, raw: bytes):
        """The `upload` method uploads raw bytes to a file on WorkspaceFS with the given `filename`. This method is
        used to upload files that are not typed, i.e., they do not have a corresponding `type_ref` object."""
        with self._lock:
            dst = f"{self.install_folder()}/{filename}"
            attempt = partial(self._ws.workspace.upload, dst, raw, format=ImportFormat.AUTO, overwrite=True)
            try:
                logger.debug(f"Uploading: {dst}")
                attempt()
            except NotFound:
                parent_folder = os.path.dirname(dst)
                logger.debug(f"Creating missing folders: {parent_folder}")
                self._ws.workspace.mkdirs(parent_folder)
                attempt()
            return dst

    def upload_dbfs(self, filename: str, raw: bytes) -> str:
        """The `upload_dbfs` method uploads raw bytes to a file on DBFS (Databricks File System) with the given
        `filename`. This method is used to upload files to DBFS, which is a distributed file system that is integrated
        with Databricks."""
        with self._lock:
            dst = f"{self.install_folder()}/{filename}"
            attempt = partial(self._ws.dbfs.upload, dst, raw, overwrite=True)
            try:
                logger.debug(f"Uploading to DBFS: {dst}")
                attempt()
            except NotFound:
                parent_folder = os.path.dirname(dst)
                logger.debug(f"Creating missing DBFS folders: {parent_folder}")
                self._ws.dbfs.mkdirs(parent_folder)
                attempt()
            return dst

    def files(self) -> list[workspace.ObjectInfo]:
        return list(self._ws.workspace.list(self.install_folder(), recursive=True))

    def remove(self):
        self._ws.workspace.delete(self.install_folder(), recursive=True)

    def workspace_link(self, path: str) -> str:
        """Returns a link to a file in a workspace.

        Usage:
            >>> import webbrowser
            >>> installation = Installation.current()
            >>> webbrowser.open(installation.workspace_link('config.yml'))
        """
        return f"{self._host()}/#workspace{self.install_folder()}/{path.removeprefix('/')}"

    def workspace_markdown_link(self, label: str, path: str) -> str:
        return f"[{label}]({self.workspace_link(path)})"

    def _host(self):
        return self._ws.config.host

    def _overwrite_content(self, filename: str, as_dict: Json, type_ref: type):
        """The `_overwrite_content` method is a private method that is used to serialize an object of type `type_ref`
        to a dictionary and write it to a file on WorkspaceFS. This method is called by the `save` and `upload` methods.

        The `as_dict` argument is the dictionary representation of the object that is to be written to the file.
        The `type_ref` argument is the type of the object that is being saved."""
        converters: dict[str, Callable[[Any, type], bytes]] = {
            "json": self._dump_json,
            "yml": self._dump_yaml,
            "csv": self._dump_csv,
        }
        extension = filename.split(".")[-1]
        if extension not in converters:
            raise KeyError(f"Unknown extension: {extension}")
        logger.debug(f"Converting {type_ref.__name__} into {extension.upper()} format")
        raw = converters[extension](as_dict, type_ref)
        self.upload(filename, raw)

    @classmethod
    def _unmarshal_type(cls, as_dict, filename, type_ref):
        expected_version = None
        if hasattr(type_ref, "__version__"):
            expected_version = getattr(type_ref, "__version__")
        if expected_version:
            as_dict = cls._migrate_file_format(type_ref, expected_version, as_dict, filename)
        return cls._unmarshal(as_dict, [], type_ref)

    def _load_content(self, filename: str) -> Json:
        with self._lock:
            # TODO: check how to make this fail fast during unit testing, otherwise
            # this currently hangs with the real installation class and mocked workspace client
            with self._ws.workspace.download(f"{self.install_folder()}/{filename}") as f:
                return self._convert_content(filename, f)

    @classmethod
    def _convert_content(cls, filename: str, raw: BinaryIO) -> Json:
        converters: dict[str, Callable[[BinaryIO], Any]] = {
            "json": json.load,
            "yml": cls._load_yaml,
            "csv": cls._load_csv,
        }
        extension = filename.split(".")[-1]
        if extension not in converters:
            raise KeyError(f"Unknown extension: {extension}")
        try:
            return converters[extension](raw)
        except JSONDecodeError:
            return {}

    def __repr__(self):
        return self.install_folder()

    @staticmethod
    def _user_home_installation(ws: WorkspaceClient, product: str):
        me = ws.current_user.me()
        return f"/Users/{me.user_name}/.{product}"

    @staticmethod
    def _migrate_file_format(type_ref, expected_version, as_dict, filename):
        actual_version = as_dict.pop("$version", 1)
        while actual_version < expected_version:
            migrate = getattr(type_ref, f"v{actual_version}_migrate", None)
            if not migrate:
                break
            as_dict = migrate(as_dict)
            prev_version = actual_version
            actual_version = as_dict.pop("$version", 1)
            if actual_version == prev_version:
                raise IllegalState(f"cannot migrate {filename} from v{prev_version}")
        if actual_version != expected_version:
            raise IllegalState(f"expected state $version={expected_version}, got={actual_version}")
        return as_dict

    @staticmethod
    def _get_filename(filename: str | None, type_ref: type) -> str:
        if not filename and hasattr(type_ref, "__file__"):
            return getattr(type_ref, "__file__")
        if not filename:
            kebab_name = re.sub(r"(?<!^)(?=[A-Z])", "-", type_ref.__name__).lower()
            filename = f"{kebab_name}.json"
        return filename

    @classmethod
    def _get_type_ref(cls, inst) -> type:
        type_ref = type(inst)
        if type_ref == list:
            return cls._get_list_type_ref(inst)
        return type_ref

    @staticmethod
    def _get_list_type_ref(inst: T) -> type[list[T]]:
        from_list: list = inst  # type: ignore[assignment]
        if len(from_list) == 0:
            raise ValueError("List cannot be empty")
        item_type = type(from_list[0])  # type: ignore[misc]
        return list[item_type]  # type: ignore[valid-type]

    @classmethod
    def _marshal(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        # pylint: disable-next=import-outside-toplevel
        from typing import (  # type: ignore[attr-defined]
            _GenericAlias,
            _UnionGenericAlias,
        )

        if dataclasses.is_dataclass(type_ref):
            return cls._marshal_dataclass(type_ref, path, inst)
        if isinstance(type_ref, types.GenericAlias):
            return cls._marshal_generic(type_ref, path, inst)
        if isinstance(type_ref, (types.UnionType, _UnionGenericAlias)):  # type: ignore[attr-defined]
            return cls._marshal_union(type_ref, path, inst)
        if isinstance(type_ref, _GenericAlias):  # type: ignore[attr-defined]
            return cls._marshal_generic_alias(type_ref, inst)
        if isinstance(inst, databricks.sdk.core.Config):
            return inst.as_dict(), True
        if type_ref == list:
            return cls._marshal_list(type_ref, path, inst)
        if isinstance(type_ref, enum.EnumMeta):
            return cls._marshal_enum(inst)
        if type_ref == types.NoneType:
            return inst, inst is None
        if type_ref == databricks.sdk.core.Config:
            return cls._marshal_databricks_config(inst)
        if type_ref in cls._PRIMITIVES:
            return inst, True
        raise SerdeError(f'{".".join(path)}: unknown: {inst}')

    @classmethod
    def _marshal_union(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        combo = []
        for variant in get_args(type_ref):
            value, ok = cls._marshal(variant, [*path, f"(as {variant})"], inst)
            if ok:
                return value, True
            combo.append(cls._explain_why(variant, [*path, f"(as {variant})"], inst))
        raise SerdeError(f'{".".join(path)}: union: {" or ".join(combo)}')

    @classmethod
    def _marshal_generic(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        type_args = get_args(type_ref)
        if not type_args:
            raise SerdeError(f"Missing type arguments: {type_args}")
        if len(type_args) == 2:
            return cls._marshal_dict(type_args[1], path, inst)
        return cls._marshal_list(type_args[0], path, inst)

    @staticmethod
    def _marshal_generic_alias(type_ref, inst):
        if not inst:
            return None, False
        return inst, isinstance(inst, type_ref.__origin__)  # type: ignore[attr-defined]

    @classmethod
    def _marshal_list(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        as_list = []
        if not inst:
            return None, False
        for i, v in enumerate(inst):
            value, ok = cls._marshal(type_ref, [*path, f"{i}"], v)
            if not ok:
                raise SerdeError(cls._explain_why(type_ref, [*path, f"{i}"], v))
            as_list.append(value)
        return as_list, True

    @classmethod
    def _marshal_dict(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        if not isinstance(inst, dict):
            return None, False
        as_dict = {}
        for k, v in inst.items():
            as_dict[k], ok = cls._marshal(type_ref, [*path, k], v)
            if not ok:
                raise SerdeError(cls._explain_why(type_ref, [*path, k], v))
        return as_dict, True

    @classmethod
    def _marshal_dataclass(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        if inst is None:
            return None, False
        as_dict = {}
        for field, hint in get_type_hints(type_ref).items():
            raw = getattr(inst, field)
            value, ok = cls._marshal(hint, [*path, field], raw)
            if not ok:
                raise SerdeError(cls._explain_why(hint, [*path, field], raw))
            if not value:
                continue
            as_dict[field] = value
        return as_dict, True

    @staticmethod
    def _marshal_databricks_config(inst):
        if not inst:
            return None, False
        return inst.as_dict(), True

    @staticmethod
    def _marshal_enum(inst):
        if not inst:
            return None, False
        return inst.value, True

    @classmethod
    def _unmarshal(cls, inst: Any, path: list[str], type_ref: type[T]) -> T | None:
        # pylint: disable-next=import-outside-toplevel
        from typing import (  # type: ignore[attr-defined]
            _GenericAlias,
            _UnionGenericAlias,
        )

        if dataclasses.is_dataclass(type_ref):
            return cls._unmarshal_dataclass(inst, path, type_ref)
        if isinstance(type_ref, (types.UnionType, _UnionGenericAlias)):
            return cls._unmarshal_union(inst, path, type_ref)
        if isinstance(type_ref, types.GenericAlias):
            return cls._unmarshal_generic(inst, path, type_ref)
        if isinstance(type_ref, _GenericAlias):
            if not inst:
                return None
            return cls._unmarshal(inst, path, type_ref.__origin__)
        if isinstance(type_ref, enum.EnumMeta):
            if not inst:
                return None
            return type_ref(inst)
        if type_ref in cls._PRIMITIVES:
            return cls._unmarshal_primitive(inst, type_ref)
        if type_ref == databricks.sdk.core.Config:
            if not inst:
                inst = {}
            return databricks.sdk.core.Config(**inst)  # type: ignore[return-value]
        if type_ref == types.NoneType:
            return None
        raise SerdeError(f'{".".join(path)}: unknown: {type_ref}: {inst}')

    @classmethod
    def _unmarshal_dataclass(cls, inst, path, type_ref):
        if inst is None:
            return None
        if not isinstance(inst, dict):
            raise SerdeError(cls._explain_why(dict, path, inst))
        from_dict = {}
        fields = getattr(type_ref, "__dataclass_fields__")
        for field_name, hint in get_type_hints(type_ref).items():
            raw = inst.get(field_name)
            value = cls._unmarshal(raw, [*path, field_name], hint)
            if value is None:
                field = fields.get(field_name)
                default_value = field.default
                default_factory = field.default_factory
                if default_factory == dataclasses.MISSING and default_value == dataclasses.MISSING:
                    raise SerdeError(cls._explain_why(hint, [*path, field_name], value))
                if default_value != dataclasses.MISSING:
                    value = default_value
                else:
                    value = default_factory()
            from_dict[field_name] = value
        return type_ref(**from_dict)

    @classmethod
    def _unmarshal_union(cls, inst, path, type_ref):
        for variant in get_args(type_ref):
            value = cls._unmarshal(inst, path, variant)
            if value:
                return value
        return None

    @classmethod
    def _unmarshal_generic(cls, inst, path, type_ref):
        type_args = get_args(type_ref)
        if not type_args:
            raise SerdeError(f"Missing type arguments: {type_args}")
        if len(type_args) == 2:
            return cls._unmarshal_dict(inst, path, type_args[1])
        return cls._unmarshal_list(inst, path, type_args[0])

    @classmethod
    def _unmarshal_list(cls, inst, path, hint):
        if inst is None:
            return None
        as_list = []
        for i, v in enumerate(inst):
            as_list.append(cls._unmarshal(v, [*path, f"{i}"], hint))
        return as_list

    @classmethod
    def _unmarshal_dict(cls, inst, path, type_ref):
        if not inst:
            return None
        if not isinstance(inst, dict):
            raise SerdeError(cls._explain_why(type_ref, path, inst))
        from_dict = {}
        for k, v in inst.items():
            from_dict[k] = cls._unmarshal(v, [*path, k], type_ref)
        return from_dict

    @classmethod
    def _unmarshal_primitive(cls, inst, type_ref):
        if not inst:
            return inst
        # convert from str to int if necessary
        converted = type_ref(inst)  # type: ignore[call-arg]
        return converted

    @staticmethod
    def _explain_why(type_ref: type, path: list[str], raw: Any) -> str:
        if raw is None:
            raw = "value is missing"
        return f'{".".join(path)}: not a {type_ref.__name__}: {raw}'

    @staticmethod
    def _dump_json(as_dict: Json, _: type) -> bytes:
        return json.dumps(as_dict, indent=2).encode("utf8")

    @staticmethod
    def _dump_yaml(raw: Json, _: type) -> bytes:
        try:
            return yaml.dump(raw).encode("utf8")
        except ImportError as err:
            raise SyntaxError("PyYAML is not installed. Fix: pip install databricks-labs-blueprint[yaml]") from err

    @staticmethod
    def _load_yaml(raw: BinaryIO) -> Json:
        try:
            try:
                return yaml.safe_load(raw)
            except yaml.YAMLError as err:
                raise JSONDecodeError(str(err), "<yaml>", 0) from err
        except ImportError as err:
            raise SyntaxError("PyYAML is not installed. Fix: pip install databricks-labs-blueprint[yaml]") from err

    @staticmethod
    def _dump_csv(raw: list[Json], type_ref: type) -> bytes:
        type_args = get_args(type_ref)
        if not type_args:
            raise SerdeError(f"Writing CSV is only supported for lists. Got {type_ref}")
        dataclass_ref = type_args[0]
        if not dataclasses.is_dataclass(dataclass_ref):
            raise SerdeError(f"Only lists of dataclasses allowed. Got {dataclass_ref}")
        non_empty_keys = set()
        for as_dict in raw:
            if not isinstance(as_dict, dict):
                raise SerdeError(f"Expecting a list of dictionaries. Got {as_dict}")
            for k, v in as_dict.items():
                if not v:
                    continue
                non_empty_keys.add(k)
        buffer = io.StringIO()
        # get ordered field names the way they appear in dataclass
        field_names = [_.name for _ in dataclasses.fields(dataclass_ref) if _.name in non_empty_keys]
        writer = csv.DictWriter(buffer, field_names, dialect="excel")
        writer.writeheader()
        for as_dict in raw:
            writer.writerow(as_dict)
        buffer.seek(0)
        return buffer.read().encode("utf8")

    @staticmethod
    def _load_csv(raw: BinaryIO) -> list[Json]:
        out = []
        for row in csv.DictReader(raw):  # type: ignore[arg-type]
            out.append(row)
        return out


class MockInstallation(Installation):
    """Install state testing toolbelt

    register with PyTest:

        pytest.register_assert_rewrite('databricks.labs.blueprint.installation')
    """

    def __init__(self, overwrites: Any = None):  # pylint: disable=super-init-not-called
        if not overwrites:
            overwrites = {}
        self._overwrites = overwrites
        self._uploads: dict[str, bytes] = {}
        self._dbfs: dict[str, bytes] = {}
        self._removed = False

    def install_folder(self) -> str:
        return "~/mock"

    def _host(self):
        return "https://localhost"

    def upload(self, filename: str, raw: bytes):
        self._uploads[filename] = raw
        return f"{self.install_folder()}/{filename}"

    def upload_dbfs(self, filename: str, raw: bytes) -> str:
        self._dbfs[filename] = raw
        return f"{self.install_folder()}/{filename}"

    def files(self) -> list[workspace.ObjectInfo]:
        out = []
        for filename in self._overwrites.keys():
            out.append(
                workspace.ObjectInfo(
                    path=f"{self.install_folder()}/{filename}",
                    object_type=workspace.ObjectType.FILE,
                )
            )
        for filename in self._uploads:
            out.append(
                workspace.ObjectInfo(
                    path=f"{self.install_folder()}/{filename}",
                    object_type=workspace.ObjectType.FILE,
                )
            )
        return out

    def remove(self):
        self._removed = True

    def _overwrite_content(self, filename: str, as_dict: Json, type_ref: type):
        self._overwrites[filename] = as_dict

    def _load_content(self, filename: str) -> Json:
        if filename not in self._overwrites:
            raise NotFound(filename)
        return self._overwrites[filename]

    def assert_file_written(self, filename: str, expected: Any):
        assert filename in self._overwrites, f"{filename} had no writes"
        actual = self._overwrites[filename]
        assert expected == actual, f"{filename} content missmatch"

    def assert_file_uploaded(self, filename):
        self._assert_upload(filename, self._uploads)

    def assert_file_dbfs_uploaded(self, filename):
        self._assert_upload(filename, self._dbfs)

    def assert_removed(self):
        assert self._removed

    @staticmethod
    def _assert_upload(filename: Any, loc: dict[str, bytes]):
        if isinstance(filename, re.Pattern):
            for name in loc.keys():
                if filename.match(name):
                    return
            raise AssertionError(f'Cannot find {filename.pattern} among {", ".join(loc.keys())}')
        assert filename in loc, f"{filename} had no writes"
