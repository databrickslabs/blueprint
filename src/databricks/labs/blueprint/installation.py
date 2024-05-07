"""The `Installation` class is used to manage the `~/.{product}` folder on WorkspaceFS to track typed files."""

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
import typing
from collections.abc import Callable, Collection
from functools import partial
from json import JSONDecodeError
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Protocol,
    TypeVar,
    get_args,
    get_type_hints,
    runtime_checkable,
)

import databricks.sdk.core
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.mixins import workspace
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.blueprint.parallel import Threads

logger = logging.getLogger(__name__)

Json = dict[str, Any]

__all__ = ["Installation", "MockInstallation", "IllegalState", "NotInstalled", "SerdeError"]


class IllegalState(ValueError):
    pass


class NotInstalled(NotFound):
    """Raised when a product is not installed."""


class SerdeError(TypeError):
    """Raised when a serialization or deserialization error occurs."""


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
        """The `Installation` class constructor creates an `Installation` object for the given product in
        the current workspace."""
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
        applications_folder = cls._global_installation(product)
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
    def assume_user_home(cls, ws: WorkspaceClient, product: str):
        """Constructs installation with explicit location in the user home folder"""
        user_folder = cls._user_home_installation(ws, product)
        return cls(ws, product, install_folder=user_folder)

    @classmethod
    def assume_global(cls, ws: WorkspaceClient, product: str):
        """Constructs installation with explicit global location in the /Applications folder"""
        applications_folder = cls._global_installation(product)
        return cls(ws, product, install_folder=applications_folder)

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

        tasks = [functools.partial(check_folder, cls._global_installation(product))]
        for user in ws.users.list(attributes="userName"):
            service_principal_folder = f"/Users/{user.user_name}/.{product}"
            tasks.append(functools.partial(check_folder, service_principal_folder))
        for service_principal in ws.service_principals.list(attributes="applicationId"):
            service_principal_folder = f"/Users/{service_principal.application_id}/.{product}"
            tasks.append(functools.partial(check_folder, service_principal_folder))
        return Threads.strict(f"finding {product} installations", tasks)

    @classmethod
    def load_local(cls, type_ref: type[T], file: Path) -> T:
        """Loads a typed file from the local file system."""
        with file.open("rb") as f:
            as_dict = cls._convert_content(file.name, f)
            return cls._unmarshal_type(as_dict, file.name, type_ref)

    def product(self) -> str:
        """The `product` method returns the name of the product associated with the installation."""
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

    def is_global(self) -> bool:
        """Returns true if current installation is in /Applications folder"""
        return self.install_folder() == self._global_installation(self.product())

    def username(self) -> str:
        """Returns the username associated with the installation folder"""
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

    def load_or_default(self, type_ref: type[T]) -> T:
        """If the file is not found, the method will return a default instance of the `type_ref` class."""
        try:
            return self.load(type_ref)
        except NotFound:
            filename = self._get_filename(None, type_ref)
            return self._unmarshal_type({}, filename, type_ref)

    def save(self, inst: T, *, filename: str | None = None):
        """The `save` method saves a dataclass object of type `T` to a file on WorkspaceFS.
        If no `filename` is provided, the name of the `type_ref` class will be used as the filename.
        If the object has a `__version__` attribute, the method will add a `version` field to the serialized object
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
            as_dict["version"] = version
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
            except NotFound as error:
                if error.error_code == "FEATURE_DISABLED":
                    self._enable_files_in_repos()
                parent_folder = os.path.dirname(dst)
                logger.debug(f"Creating missing folders: {parent_folder}")
                self._ws.workspace.mkdirs(parent_folder)
                attempt()
            return self._strip_notebook_source_suffix(dst, raw)

    _NOTEBOOK_MAGIC = {
        "py": b"# Databricks notebook source",
        "scala": b"// Databricks notebook source",
        "sql": b"-- Databricks notebook source",
    }

    @classmethod
    def _strip_notebook_source_suffix(cls, dst: str, raw: bytes) -> str:
        """If the file is a Databricks notebook, the method will remove the suffix from the filename."""
        if "." not in dst:
            return dst
        ext = dst.split(".")[-1]
        magic = cls._NOTEBOOK_MAGIC.get(ext)
        if not magic:
            return dst
        magic_len = len(magic)
        if len(raw) > magic_len and raw[0:magic_len] == magic:
            return dst.removesuffix(f".{ext}")
        return dst

    def upload_dbfs(self, filename: str, raw: BinaryIO) -> str:
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
        """The `files` method returns a list of all files in the installation folder on WorkspaceFS.
        This method is used to list the files that are managed by the `Installation` class."""
        return list(self._ws.workspace.list(self.install_folder(), recursive=True))

    def remove(self):
        """The `remove` method deletes the installation folder on WorkspaceFS.
        This method is used to remove all files and folders that are managed by the `Installation` class."""
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
        """Returns a markdown link to a file in a workspace."""
        return f"[{label}]({self.workspace_link(path)})"

    def _host(self):
        """Returns the host of the current workspace."""
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

    @staticmethod
    def _global_installation(product):
        """The `_global_installation` method is a private method that is used to determine the installation folder
        for the given product in the `/Applications` directory. This method is called by the `install_folder` method."""
        return f"/Applications/{product}"

    @classmethod
    def _unmarshal_type(cls, as_dict, filename, type_ref):
        """The `_unmarshal_type` method is a private method that is used to deserialize a dictionary to an object of
        type `type_ref`. This method is called by the `load` method."""
        expected_version = None
        if hasattr(type_ref, "__version__"):
            expected_version = getattr(type_ref, "__version__")
        if expected_version:
            as_dict = cls._migrate_file_format(type_ref, expected_version, as_dict, filename)
        return cls._unmarshal(as_dict, [], type_ref)

    def _load_content(self, filename: str) -> Json:
        """The `_load_content` method is a private method that is used to load the contents of a file from
        WorkspaceFS as a dictionary. This method is called by the `load` method."""
        with self._lock:
            # TODO: check how to make this fail fast during unit testing, otherwise
            # this currently hangs with the real installation class and mocked workspace client
            with self._ws.workspace.download(f"{self.install_folder()}/{filename}") as f:
                return self._convert_content(filename, f)

    @classmethod
    def _convert_content(cls, filename: str, raw: BinaryIO) -> Json:
        """The `_convert_content` method is a private method that is used to convert the raw bytes of a file to a
        dictionary. This method is called by the `_load_content` method."""
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

    def __eq__(self, other):
        if not isinstance(other, Installation):
            return False
        return self.install_folder() == other.install_folder()

    def __hash__(self):
        """The `__hash__` method is used to hash the `Installation` object.
        This method is called by the `hash` function."""
        return hash(self.install_folder())

    @staticmethod
    def _user_home_installation(ws: WorkspaceClient, product: str):
        """The `_user_home_installation` method is a private method that is used to determine the installation folder
        for the current user. This method is called by the `install_folder` method."""
        me = ws.current_user.me()
        return f"/Users/{me.user_name}/.{product}"

    @staticmethod
    def _migrate_file_format(type_ref, expected_version, as_dict, filename):
        """The `_migrate_file_format` method is a private method that is used to migrate the file format of a file"""
        actual_version = as_dict.pop("version", 1)
        while actual_version < expected_version:
            migrate = getattr(type_ref, f"v{actual_version}_migrate", None)
            if not migrate:
                break
            as_dict = migrate(as_dict)
            prev_version = actual_version
            actual_version = as_dict.pop("version", 1)
            if actual_version == prev_version:
                raise IllegalState(f"cannot migrate {filename} from v{prev_version}")
        if actual_version != expected_version:
            raise IllegalState(f"expected state version={expected_version}, got={actual_version}")
        return as_dict

    @staticmethod
    def _get_filename(filename: str | None, type_ref: type) -> str:
        """The `_get_filename` method is a private method that is used to determine the filename of a file based on
        the type of the object that is being saved. This method is called by the `save` method."""
        if not filename and hasattr(type_ref, "__file__"):
            return getattr(type_ref, "__file__")
        if not filename:
            kebab_name = re.sub(r"(?<!^)(?=[A-Z])", "-", type_ref.__name__).lower()
            filename = f"{kebab_name}.json"
        return filename

    @classmethod
    def _get_type_ref(cls, inst) -> type:
        """The `_get_type_ref` method is a private method that is used to determine the type of an object. This method
        is called by the `save` method."""
        type_ref = type(inst)
        if type_ref == list:
            return cls._get_list_type_ref(inst)
        return type_ref

    @staticmethod
    def _get_list_type_ref(inst: T) -> type[list[T]]:
        """The `_get_list_type_ref` method is a private method that is used to determine the type of a list object."""
        from_list: list = inst  # type: ignore[assignment]
        if len(from_list) == 0:
            raise ValueError("List cannot be empty")
        item_type = type(from_list[0])  # type: ignore[misc]
        return list[item_type]  # type: ignore[valid-type]

    @classmethod
    def _marshal(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        """The `_marshal` method is a private method that is used to serialize an object of type `type_ref` to
        a dictionary. This method is called by the `save` method."""
        if hasattr(inst, "as_dict"):
            return inst.as_dict(), True
        if dataclasses.is_dataclass(type_ref):
            return cls._marshal_dataclass(type_ref, path, inst)
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
        return cls._marshal_generic_types(type_ref, path, inst)

    @classmethod
    def _marshal_generic_types(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        # pylint: disable-next=import-outside-toplevel,import-private-name
        from typing import (  # type: ignore[attr-defined]
            _GenericAlias,
            _UnionGenericAlias,
        )

        if isinstance(type_ref, (types.UnionType, _UnionGenericAlias)):  # type: ignore[attr-defined]
            return cls._marshal_union(type_ref, path, inst)
        if isinstance(type_ref, (_GenericAlias, types.GenericAlias)):  # type: ignore[attr-defined]
            if type_ref.__origin__ in (dict, list) or isinstance(type_ref, types.GenericAlias):
                return cls._marshal_generic(type_ref, path, inst)
            return cls._marshal_generic_alias(type_ref, inst)
        raise SerdeError(f'{".".join(path)}: unknown: {inst}')

    @classmethod
    def _marshal_union(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        """The `_marshal_union` method is a private method that is used to serialize an object of type `type_ref` to
        a dictionary. This method is called by the `save` method."""
        combo = []
        for variant in get_args(type_ref):
            value, ok = cls._marshal(variant, [*path, f"(as {variant})"], inst)
            if ok:
                return value, True
            combo.append(cls._explain_why(variant, [*path, f"(as {variant})"], inst))
        raise SerdeError(f'{".".join(path)}: union: {" or ".join(combo)}')

    @classmethod
    def _marshal_generic(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        """The `_marshal_generic` method is a private method that is used to serialize an object of type `type_ref`
        to a dictionary. This method is called by the `save` method."""
        type_args = get_args(type_ref)
        if not type_args:
            raise SerdeError(f"Missing type arguments: {type_args}")
        if len(type_args) == 2:
            return cls._marshal_dict(type_args[1], path, inst)
        return cls._marshal_list(type_args[0], path, inst)

    @staticmethod
    def _marshal_generic_alias(type_ref, inst):
        """The `_marshal_generic_alias` method is a private method that is used to serialize an object of type
        `type_ref` to a dictionary. This method is called by the `save` method."""
        if not inst:
            return None, False
        return inst, isinstance(inst, type_ref.__origin__)  # type: ignore[attr-defined]

    @classmethod
    def _marshal_list(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        """The `_marshal_list` method is a private method that is used to serialize an object of type `type_ref` to
        a dictionary. This method is called by the `save` method."""
        as_list = []
        if not isinstance(inst, list):
            return None, False
        for i, v in enumerate(inst):
            value, ok = cls._marshal(type_ref, [*path, f"{i}"], v)
            if not ok:
                raise SerdeError(cls._explain_why(type_ref, [*path, f"{i}"], v))
            as_list.append(value)
        return as_list, True

    @classmethod
    def _marshal_dict(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        """The `_marshal_dict` method is a private method that is used to serialize an object of type `type_ref` to
        a dictionary. This method is called by the `save` method."""
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
        """The `_marshal_dataclass` method is a private method that is used to serialize an object of type `type_ref`
        to a dictionary. This method is called by the `save` method."""
        if inst is None:
            return None, False
        as_dict = {}
        for field, hint in get_type_hints(type_ref).items():
            origin = getattr(hint, "__origin__", None)
            if origin is typing.ClassVar:
                continue
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
        """The `_marshal_databricks_config` method is a private method that is used to serialize an object of type
        `databricks.sdk.core.Config` to a dictionary. This method is called by the `save` method."""
        if not inst:
            return None, False
        return inst.as_dict(), True

    @staticmethod
    def _marshal_enum(inst):
        """The `_marshal_enum` method is a private method that is used to serialize an object of type `enum.Enum` to
        a dictionary. This method is called by the `save` method."""
        if not inst:
            return None, False
        return inst.value, True

    @runtime_checkable
    class _FromDict(Protocol):
        """The `_FromDict` protocol is used to define a type that can be constructed from a dictionary. This protocol
        is used to define a type that can be constructed from a dictionary. This protocol is used to define a type that
        can be constructed from a dictionary."""

        @classmethod
        def from_dict(cls, raw: dict):
            pass

    @classmethod
    def _unmarshal(cls, inst: Any, path: list[str], type_ref: type[T]) -> T | None:
        """The `_unmarshal` method is a private method that is used to deserialize a dictionary to an object of type
        `type_ref`. This method is called by the `load` method."""
        if dataclasses.is_dataclass(type_ref):
            return cls._unmarshal_dataclass(inst, path, type_ref)
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
        if isinstance(type_ref, cls._FromDict):
            return type_ref.from_dict(inst)
        return cls._unmarshal_generic_types(type_ref, path, inst)

    @classmethod
    def _unmarshal_generic_types(cls, type_ref, path, inst):
        # pylint: disable-next=import-outside-toplevel,import-private-name
        from typing import (  # type: ignore[attr-defined]
            _GenericAlias,
            _UnionGenericAlias,
        )

        if isinstance(type_ref, (types.UnionType, _UnionGenericAlias)):
            return cls._unmarshal_union(inst, path, type_ref)
        if isinstance(type_ref, (_GenericAlias, types.GenericAlias)):
            return cls._unmarshal_generic(inst, path, type_ref)
        raise SerdeError(f'{".".join(path)}: unknown: {type_ref}: {inst}')

    @classmethod
    def _unmarshal_dataclass(cls, inst, path, type_ref):
        """The `_unmarshal_dataclass` method is a private method that is used to deserialize a dictionary to an object
        of type `type_ref`. This method is called by the `load` method."""
        if inst is None:
            return None
        if not isinstance(inst, dict):
            raise SerdeError(cls._explain_why(dict, path, inst))
        from_dict = {}
        fields = getattr(type_ref, "__dataclass_fields__")
        for field_name, hint in get_type_hints(type_ref).items():
            origin = getattr(hint, "__origin__", None)
            if origin is typing.ClassVar:
                continue
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
        """The `_unmarshal_union` method is a private method that is used to deserialize a dictionary to an object
        of type `type_ref`. This method is called by the `load` method."""
        for variant in get_args(type_ref):
            value = cls._unmarshal(inst, path, variant)
            if value:
                return value
        return None

    @classmethod
    def _unmarshal_generic(cls, inst, path, type_ref):
        """The `_unmarshal_generic` method is a private method that is used to deserialize a dictionary to an object
        of type `type_ref`. This method is called by the `load` method."""
        # pylint: disable-next=import-outside-toplevel,import-private-name
        from typing import _GenericAlias  # type: ignore[attr-defined]

        type_args = get_args(type_ref)
        if not type_args:
            raise SerdeError(f"Missing type arguments: {type_args}")
        if type_ref.__origin__ not in (dict, list) and isinstance(type_ref, _GenericAlias):
            return cls._unmarshal(inst, path, type_ref.__origin__)
        if inst is None:
            return None
        if len(type_args) == 2:
            return cls._unmarshal_dict(inst, path, type_args[1])
        return cls._unmarshal_list(inst, path, type_args[0])

    @classmethod
    def _unmarshal_list(cls, inst, path, hint):
        """The `_unmarshal_list` method is a private method that is used to deserialize a dictionary to an object
        of type `type_ref`. This method is called by the `load` method."""
        if inst is None:
            return None
        as_list = []
        for i, v in enumerate(inst):
            as_list.append(cls._unmarshal(v, [*path, f"{i}"], hint))
        return as_list

    @classmethod
    def _unmarshal_dict(cls, inst, path, type_ref):
        """The `_unmarshal_dict` method is a private method that is used to deserialize a dictionary to an object
        of type `type_ref`. This method is called by the `load` method."""
        if inst is None:
            return None
        if not isinstance(inst, dict):
            raise SerdeError(cls._explain_why(type_ref, path, inst))
        from_dict = {}
        for k, v in inst.items():
            from_dict[k] = cls._unmarshal(v, [*path, k], type_ref)
        return from_dict

    @classmethod
    def _unmarshal_primitive(cls, inst, type_ref):
        """The `_unmarshal_primitive` method is a private method that is used to deserialize a dictionary to an object
        of type `type_ref`. This method is called by the `load` method."""
        if not inst:
            return inst
        # convert from str to int if necessary
        converted = type_ref(inst)  # type: ignore[call-arg]
        return converted

    @staticmethod
    def _explain_why(type_ref: type, path: list[str], raw: Any) -> str:
        """The `_explain_why` method is a private method that is used to explain why a value is not of the expected
        type. This method is called by the `_unmarshal` and `_marshal` methods."""
        if raw is None:
            raw = "value is missing"
        return f'{".".join(path)}: not a {type_ref.__name__}: {raw}'

    @staticmethod
    def _dump_json(as_dict: Json, _: type) -> bytes:
        """The `_dump_json` method is a private method that is used to serialize a dictionary to a JSON string. This
        method is called by the `save` method."""
        return json.dumps(as_dict, indent=2).encode("utf8")

    @staticmethod
    def _dump_yaml(raw: Json, _: type) -> bytes:
        """The `_dump_yaml` method is a private method that is used to serialize a dictionary to a YAML string. This
        method is called by the `save` method."""
        try:
            from yaml import dump  # pylint: disable=import-outside-toplevel

            return dump(raw).encode("utf8")
        except ImportError as err:
            raise SyntaxError("PyYAML is not installed. Fix: pip install databricks-labs-blueprint[yaml]") from err

    @staticmethod
    def _load_yaml(raw: BinaryIO) -> Json:
        """The `_load_yaml` method is a private method that is used to deserialize a YAML string to a dictionary. This
        method is called by the `load` method."""
        try:
            from yaml import (  # pylint: disable=import-outside-toplevel
                YAMLError,
                safe_load,
            )

            try:
                return safe_load(raw)
            except YAMLError as err:
                raise JSONDecodeError(str(err), "<yaml>", 0) from err
        except ImportError as err:
            raise SyntaxError("PyYAML is not installed. Fix: pip install databricks-labs-blueprint[yaml]") from err

    @staticmethod
    def _dump_csv(raw: list[Json], type_ref: type) -> bytes:
        """The `_dump_csv` method is a private method that is used to serialize a list of dictionaries to a CSV string.
        This method is called by the `save` method."""
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
        with io.TextIOWrapper(raw, encoding="utf8") as text_file:
            out = []
            for row in csv.DictReader(text_file):  # type: ignore[arg-type]
                out.append(row)
            return out

    def _enable_files_in_repos(self):
        """The `_enable_files_in_repos` method is a private method that is used to enable the "Files In Repos"
        feature on the current workspace. This method is called by the `upload` method."""
        workspace_file_system = self._ws.workspace_conf.get_status("enableWorkspaceFilesystem")

        logger.debug("Checking Files In Repos configuration")

        if workspace_file_system["enableWorkspaceFilesystem"] == "false":
            logger.debug("enableWorkspaceFilesystem is False, enabling the config")
            self._ws.workspace_conf.set_status({"enableWorkspaceFilesystem": "true"})


class MockInstallation(Installation):
    """Install state testing toolbelt

    register with PyTest:

        pytest.register_assert_rewrite('databricks.labs.blueprint.installation')
    """

    def __init__(self, overwrites: Any = None, *, is_global=True):  # pylint: disable=super-init-not-called
        if not overwrites:
            overwrites = {}
        self._overwrites = overwrites
        self._uploads: dict[str, bytes] = {}
        self._dbfs: dict[str, bytes] = {}
        self._removed = False
        self._is_global = is_global

    def install_folder(self) -> str:
        return "~/mock"

    def is_global(self) -> bool:
        return self._is_global

    def product(self) -> str:
        return "mock"

    def _host(self):
        return "https://localhost"

    def upload(self, filename: str, raw: bytes):
        self._uploads[filename] = raw
        return f"{self.install_folder()}/{filename}"

    def upload_dbfs(self, filename: str, raw: BinaryIO) -> str:
        self._dbfs[filename] = raw.read()
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
        if isinstance(expected, dict):
            for k, v in expected.items():
                if v == ...:
                    self._overwrites[filename][k] = ...
        actual = self._overwrites[filename]
        assert expected == actual, f"{filename} content missmatch"

    def assert_file_uploaded(self, filename, expected: bytes | None = None):
        """Asserts that a file was uploaded with the expected content"""
        self._assert_upload(filename, self._uploads, expected)

    def assert_file_dbfs_uploaded(self, filename, expected: bytes | None = None):
        """Asserts that a file was uploaded to DBFS with the expected content"""
        self._assert_upload(filename, self._dbfs, expected)

    def assert_removed(self):
        assert self._removed

    @staticmethod
    def _assert_upload(filename: Any, loc: dict[str, bytes], expected: bytes | None = None):
        if isinstance(filename, re.Pattern):
            for name in loc.keys():
                if not filename.match(name):
                    continue
                if expected:
                    assert loc[name] == expected, f"{filename} content missmatch"
                return
            raise AssertionError(f'Cannot find {filename.pattern} among {", ".join(loc.keys())}')
        assert filename in loc, f"{filename} had no writes"
        if expected:
            assert loc[filename] == expected, f"{filename} content missmatch"
