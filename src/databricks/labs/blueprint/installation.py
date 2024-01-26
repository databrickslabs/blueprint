import csv
import dataclasses
import enum
import io
import json
import logging
import threading
import types
import typing
from functools import partial
from json import JSONDecodeError
from typing import Any

import databricks.sdk.core
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.mixins import workspace
from databricks.sdk.service.workspace import ImportFormat

logger = logging.getLogger(__name__)

Json = dict[str, Any]


class IllegalState(ValueError):
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

    _PRIMITIVES = (int, bool, float, str)

    def __init__(self, ws: WorkspaceClient, product: str, *, install_folder: str | None = None):
        self._ws = ws
        self._product = product
        self._install_folder = install_folder
        self._lock = threading.Lock()

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
        # Create an Installation object for the "myproduct" product
        install = Installation(WorkspaceClient(), "myproduct")

        # Print the path to the installation folder
        print(install.install_folder())
        # Output: /Users/{user_name}/.myproduct
        ```

        In this example, the `Installation` object is created for the "myproduct" product. The `install_folder` method
        is then called to print the path to the installation folder. The output will be `/Users/{user_name}/.myproduct`,
        where `{user_name}` is the username of the current user.

        You can also provide an `install_folder` argument to the constructor to specify a custom installation folder.
        Here is an example of how you can do this:

        ```
        # Create an Installation object for the "myproduct" product with a custom installation folder
        install = Installation(WorkspaceClient(), "myproduct", install_folder="/my/custom/folder")

        # Print the path to the installation folder
        print(install.install_folder())
        # Output: /my/custom/folder
        ```

        In this example, the `Installation` object is created for the "myproduct" product with a custom installation
        folder of `/my/custom/folder`. The `install_folder` method is then called to print the path to the installation
        folder. The output will be `/my/custom/folder`."""
        if self._install_folder:
            return self._install_folder
        me = self._ws.current_user.me()
        self._install_folder = f"/Users/{me.user_name}/.{self._product}"
        return self._install_folder

    T = typing.TypeVar("T")

    def load(self, type_ref: typing.Type[T], *, filename: str | None = None) -> T | None:
        """The `load` method loads an object of type `type_ref` from a file on WorkspaceFS. If no `filename` is
        provided, the `__file__` attribute of `type_ref` will be used as the filename.

        If the object has a `__version__` attribute, the method will check that the version of the object in the file
        matches the expected version. If the versions do not match, the method will attempt to migrate the object to
        the expected version using a method named `v{actual_version}_migrate` on the `type_ref` class. If the migration
        is successful, the method will return the migrated object. If the migration is not successful, the method will
        raise an `IllegalState` exception."""
        expected_version = None
        if hasattr(type_ref, "__version__"):
            expected_version = getattr(type_ref, "__version__")
        filename = self._get_filename(filename, type_ref)
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
        return self._unmarshal(as_dict, [], type_ref)

    @staticmethod
    def _get_filename(filename: str | None, type_ref: typing.Type) -> str:
        if not filename and hasattr(type_ref, "__file__"):
            return getattr(type_ref, "__file__")
        if not filename:
            filename = f"{type_ref.__name__}.json"
        return filename

    def save(self, inst: T, *, filename: str | None = None):
        """The `save` method saves a dataclass object of type `T` to a file on WorkspaceFS.
        If no `filename` is provided, the name of the `type_ref` class will be used as the filename.
        If the object has a `__version__` attribute, the method will add a `$version` field to the serialized object
        with the value of the `__version__` attribute.

        Here is an example of how you can use the `save` method:

        ```
        # Create an Installation object for the "myproduct" product
        install = Installation(WorkspaceClient(), "myproduct")

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

        In this example, the `Installation` object is created for the "myproduct" product. A dataclass object of type
        `MyClass` is then created and saved to a file using the `save` method. The object is then loaded from the file
        using the `load` method and compared to the original object to verify that it was saved correctly."""
        if not inst:
            raise TypeError("missing value")
        type_ref = type(inst)
        if type_ref == list:
            if len(inst) == 0:
                raise ValueError("List cannot be empty")
            type_ref = list[type(inst[0])]  # typing: ignore[misc,index]
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
                attempt()
            except NotFound:
                self._ws.workspace.mkdirs(self.install_folder())
                attempt()
            return dst

    def upload_dbfs(self, filename: str, raw: bytes) -> str:
        """The `upload_dbfs` method uploads raw bytes to a file on DBFS (Databricks File System) with the given
        `filename`. This method is used to upload files to DBFS, which is a distributed file system that is integrated
        with Databricks."""
        # TODO: use this in Wheels to upload/download random files into correct prefix in WSFS/DBFS
        with self._lock:
            dst = f"{self.install_folder()}/{filename}"
            attempt = partial(self._ws.dbfs.upload, dst, raw, overwrite=True)
            try:
                attempt()
            except NotFound:
                self._ws.dbfs.mkdirs(self.install_folder())
                attempt()
            return dst

    def files(self) -> list[workspace.ObjectInfo]:
        # TODO: list files under install folder
        raise NotImplementedError

    def _overwrite_content(self, filename: str, as_dict: Json, type_ref: typing.Type):
        """The `_overwrite_content` method is a private method that is used to serialize an object of type `type_ref`
        to a dictionary and write it to a file on WorkspaceFS. This method is called by the `save` and `upload` methods.

        The `as_dict` argument is the dictionary representation of the object that is to be written to the file.
        The `type_ref` argument is the type of the object that is being saved."""
        converters: dict[str, typing.Callable[[Any, typing.Type], bytes]] = {
            "json": self._dump_json,
            "yml": self._dump_yaml,
            "csv": self._dump_csv,
        }
        extension = filename.split(".")[-1]
        if extension not in converters:
            raise KeyError(f"Unknown extension: {extension}")
        raw = converters[extension](as_dict, type_ref)
        self.upload(filename, raw)

    def _load_content(self, filename: str) -> Json:
        with self._lock:
            converters = {"json": json.load, "yml": self._load_yaml, "csv": self._load_csv}
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

    def __repr__(self):
        return self.install_folder()

    @classmethod
    def _marshal(cls, type_ref: type, path: list[str], inst: Any) -> tuple[Any, bool]:
        if dataclasses.is_dataclass(type_ref):
            if inst is None:
                return None, False
            as_dict = {}
            for field, hint in typing.get_type_hints(type_ref).items():
                raw = getattr(inst, field)
                value, ok = cls._marshal(hint, [*path, field], raw)
                if not ok:
                    raise TypeError(cls._explain_why(hint, [*path, field], raw))
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
                value, ok = cls._marshal(hint, [*path, f"{i}"], v)
                if not ok:
                    raise TypeError(cls._explain_why(hint, [*path, f"{i}"], v))
                values.append(value)
            return values, True
        if isinstance(type_ref, (types.UnionType, typing._UnionGenericAlias)):
            combo = []
            for variant in typing.get_args(type_ref):
                value, ok = cls._marshal(variant, [*path, f"(as {variant})"], inst)
                if ok:
                    return value, True
                combo.append(cls._explain_why(variant, [*path, f"(as {variant})"], inst))
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
                value, ok = cls._marshal(hint, [*path, f"{i}"], v)
                if not ok:
                    raise TypeError(cls._explain_why(hint, [*path, f"{i}"], v))
                values.append(value)
            return values, True
        if isinstance(type_ref, enum.EnumMeta):
            if not inst:
                return None, False
            return inst.value, True
        if type_ref == types.NoneType:
            return inst, inst is None
        if type_ref == databricks.sdk.core.Config:
            if not inst:
                return None, False
            return inst.as_dict(), True
        if type_ref in cls._PRIMITIVES:
            return inst, True
        raise TypeError(f'{".".join(path)}: unknown: {inst}')

    @classmethod
    def _unmarshal(cls, inst: Any, path: list[str], type_ref: typing.Type[T]) -> T | None:
        if dataclasses.is_dataclass(type_ref):
            if inst is None:
                return None
            if not isinstance(inst, dict):
                raise TypeError(cls._explain_why(dict, path, inst))
            as_dict = {}
            fields = getattr(type_ref, "__dataclass_fields__")
            for field_name, hint in typing.get_type_hints(type_ref).items():
                raw = inst.get(field_name)
                value = cls._unmarshal(raw, [*path, field_name], hint)
                if value is None:
                    field = fields.get(field_name)
                    default_value = field.default
                    default_factory = field.default_factory
                    if default_factory == dataclasses.MISSING and default_value == dataclasses.MISSING:
                        raise TypeError(cls._explain_why(hint, [*path, field_name], value))
                    elif default_value != dataclasses.MISSING:
                        value = default_value
                    else:
                        value = default_factory()
                as_dict[field_name] = value
            return type_ref(**as_dict)
        if isinstance(type_ref, (types.UnionType, typing._UnionGenericAlias)):
            for variant in typing.get_args(type_ref):
                value = cls._unmarshal(inst, path, variant)
                if value:
                    return value
            return None
        if isinstance(type_ref, types.GenericAlias):
            type_args = typing.get_args(type_ref)
            if not type_args:
                raise TypeError(f"Missing type arguments: {type_args}")
            if len(type_args) == 2:
                if not inst:
                    return None
                if not isinstance(inst, dict):
                    raise TypeError(cls._explain_why(type_ref, path, inst))
                as_dict = {}
                for k, v in inst.items():
                    as_dict[k] = cls._unmarshal(v, [*path, k], type_args[1])
                return as_dict
            hint = type_args[0]
            if not inst:
                return None
            as_list = []
            for i, v in enumerate(inst):
                as_list.append(cls._unmarshal(v, [*path, f"{i}"], hint))
            return as_list
        if isinstance(type_ref, typing._GenericAlias):
            if not inst:
                return None
            return cls._unmarshal(inst, path, type_ref.__origin__)
        if isinstance(type_ref, enum.EnumMeta):
            if not inst:
                return None
            return type_ref(inst)
        if type_ref in cls._PRIMITIVES:
            if not inst:
                return inst
            # convert from str to int if necessary
            converted = type_ref(inst)  # typing: ignore[call-arg]
            return converted
        if type_ref == databricks.sdk.core.Config:
            if not inst:
                inst = {}
            return databricks.sdk.core.Config(**inst)  # typing: ignore[return-value]
        if type_ref == types.NoneType:
            return None
        raise TypeError(f'{".".join(path)}: unknown: {type_ref}: {inst}')

    @staticmethod
    def _explain_why(type_ref: type, path: list[str], raw: Any) -> str:
        if raw is None:
            raw = "value is missing"
        return f'{".".join(path)}: not a {type_ref.__name__}: {raw}'

    @staticmethod
    def _dump_json(as_dict: Json, _: typing.Type) -> bytes:
        return json.dumps(as_dict, indent=2).encode("utf8")

    @staticmethod
    def _dump_yaml(raw: Json, _: typing.Type) -> bytes:
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

    @staticmethod
    def _dump_csv(raw: list[Json], type_ref: typing.Type) -> bytes:
        type_args = typing.get_args(type_ref)
        if not type_args:
            raise TypeError(f"Writing CSV is only supported for lists. Got {type_ref}")
        dataclass_ref = type_args[0]
        if not dataclasses.is_dataclass(dataclass_ref):
            raise TypeError(f"Only lists of dataclasses allowed. Got {dataclass_ref}")
        non_empty_keys = set()
        for as_dict in raw:
            if not isinstance(as_dict, dict):
                raise TypeError(f"Expecting a list of dictionaries. Got {as_dict}")
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
    def _load_csv(raw: typing.BinaryIO) -> list[Json]:
        out = []
        for row in csv.DictReader(raw):  # type: ignore[arg-type]
            out.append(row)
        return out


class MockInstallation(Installation):
    """Install state testing toolbelt

    register with PyTest:

        pytest.register_assert_rewrite('databricks.labs.blueprint.installer')
    """

    def __init__(self, overwrites: Any = None):
        if not overwrites:
            overwrites = {}
        self._overwrites = overwrites

    def install_folder(self) -> str:
        return "~/mock/"

    def _overwrite_content(self, filename: str, as_dict: Json, type_ref: typing.Type):
        self._overwrites[filename] = as_dict

    def _load_content(self, filename: str) -> Json:
        return self._overwrites[filename]

    def assert_file_written(self, filename: str, expected: Any):
        assert filename in self._overwrites, f"{filename} had no writes"
        actual = self._overwrites[filename]
        assert expected == actual, f"{filename} content missmatch"