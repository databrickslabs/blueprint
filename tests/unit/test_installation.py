import dataclasses
import io
import re
import typing
from dataclasses import dataclass
from typing import TypeAlias
from unittest.mock import MagicMock, create_autospec

import pytest
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam
from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.blueprint.installation import (
    IllegalState,
    Installation,
    JsonValue,
    MockInstallation,
    SerdeError,
)


def test_current_not_found() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.get_status.side_effect = NotFound(None)

    with pytest.raises(NotFound, match="Application not installed: blueprint"):
        Installation.current(ws, "blueprint")


def test_current_not_found_assume_user() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.get_status.side_effect = NotFound(None)

    installation = Installation.current(ws, "blueprint", assume_user=True)
    assert "/Users/foo/.blueprint" == installation.install_folder()


def test_current_found_user() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.get_status.side_effect = None

    installation = Installation.current(ws, "blueprint")
    assert "/Users/foo/.blueprint" == installation.install_folder()


def test_current_found_root() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.get_status.side_effect = [NotFound(None), None]

    installation = Installation.current(ws, "blueprint")
    assert "/Applications/blueprint" == installation.install_folder()


def test_existing_not_found() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.users.list.return_value = [iam.User(user_name="foo")]
    ws.workspace.get_status.side_effect = NotFound(None)

    existing = Installation.existing(ws, "blueprint")
    assert [] == existing

    ws.workspace.get_status.assert_any_call("/Applications/blueprint")
    ws.workspace.get_status.assert_any_call("/Users/foo/.blueprint")
    assert 2 == ws.workspace.get_status.call_count


def test_existing_found_root() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.side_effect = None

    existing = Installation.existing(ws, "blueprint")
    first_existing = next(iter(existing))
    assert "/Applications/blueprint" == first_existing.install_folder()


@dataclass
class WorkspaceConfig:
    __file__ = "config.yml"
    __version__ = 2

    inventory_database: str
    connect: Config | None = None
    workspace_group_regex: str | None = None
    include_group_names: list[str] | None = None
    num_threads: int | None = 10
    database_to_catalog_mapping: dict[str, str] | None = None
    log_level: str | None = "INFO"
    workspace_start_path: str = "/"


def test_save_typed_file() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    installation = Installation(ws, "blueprint")

    target = installation.save(
        WorkspaceConfig(
            inventory_database="some_blueprint",
            include_group_names=["foo", "bar"],
        )
    )
    assert "/Users/foo/.blueprint/config.yml" == target

    ws.workspace.upload.assert_called_with(
        "/Users/foo/.blueprint/config.yml",
        yaml.dump(
            {
                "version": 2,
                "num_threads": 10,
                "inventory_database": "some_blueprint",
                "include_group_names": ["foo", "bar"],
                "workspace_start_path": "/",
                "log_level": "INFO",
            }
        ).encode("utf8"),
        format=ImportFormat.AUTO,
        overwrite=True,
    )


def test_creates_missing_folders() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.upload.side_effect = [NotFound(None), None]
    installation = Installation(ws, "blueprint")

    installation.save(WorkspaceConfig(inventory_database="some_blueprint"))

    ws.workspace.mkdirs.assert_called_with("/Users/foo/.blueprint")


def test_upload_dbfs() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    installation = Installation(ws, "blueprint")

    target = installation.upload_dbfs("wheels/foo.whl", io.BytesIO(b"abc"))
    assert "/Users/foo/.blueprint/wheels/foo.whl" == target


def test_upload_dbfs_mkdirs() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.dbfs.upload.side_effect = [NotFound(None), None]
    installation = Installation(ws, "blueprint")

    target = installation.upload_dbfs("wheels/foo.whl", io.BytesIO(b"abc"))
    assert "/Users/foo/.blueprint/wheels/foo.whl" == target

    ws.dbfs.mkdirs.assert_called_with("/Users/foo/.blueprint/wheels")


def test_save_typed_file_array_csv() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    installation = Installation(ws, "blueprint")

    installation.save(
        [
            Workspace(workspace_id=1234, workspace_name="first"),
            Workspace(workspace_id=1235, workspace_name="second"),
        ],
        filename="workspaces.csv",
    )

    ws.workspace.upload.assert_called_with(
        "/Users/foo/.blueprint/workspaces.csv",
        "\r\n".join(["workspace_id,workspace_name", "1234,first", "1235,second", ""]).encode("utf8"),
        format=ImportFormat.AUTO,
        overwrite=True,
    )


def test_load_typed_file() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value = io.StringIO(
        yaml.dump(
            {
                "version": 2,
                "num_threads": 20,
                "inventory_database": "some_blueprint",
                "connect": {"host": "https://foo", "token": "bar"},
            }
        )
    )
    installation = Installation(ws, "blueprint")

    cfg = installation.load(WorkspaceConfig)

    assert 20 == cfg.num_threads
    assert "/" == cfg.workspace_start_path


def test_load_csv_file() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value = io.BytesIO(
        "\n".join(["workspace_id,workspace_name", "1234,first", "1235,second"]).encode("utf8")
    )
    installation = Installation(ws, "blueprint")

    workspaces = installation.load(list[Workspace], filename="workspaces.csv")

    assert 2 == len(workspaces)
    assert "first" == workspaces[0].workspace_name
    assert 1235 == workspaces[1].workspace_id


@pytest.mark.parametrize("ext", ["json", "csv"])
def test_load_typed_list_file(ext) -> None:
    installation = MockInstallation(
        {
            f"workspaces.{ext}": [
                {"workspace_id": 1234, "workspace_name": "first"},
                {"workspace_id": 1235, "workspace_name": "second"},
            ]
        }
    )

    workspaces = installation.load(list[Workspace], filename=f"workspaces.{ext}")

    assert 2 == len(workspaces)
    assert "first" == workspaces[0].workspace_name
    assert 1235 == workspaces[1].workspace_id


def test_save_typed_file_array_json() -> None:
    installation = MockInstallation()

    installation.save(
        [
            Workspace(workspace_id=1234, workspace_name="first"),
            Workspace(workspace_id=1235, workspace_name="second"),
        ],
        filename="workspaces.json",
    )

    installation.assert_file_written(
        "workspaces.json",
        [{"workspace_id": 1234, "workspace_name": "first"}, {"workspace_id": 1235, "workspace_name": "second"}],
    )


def test_mock_save_typed_file() -> None:
    installation = MockInstallation()

    installation.save(WorkspaceConfig(inventory_database="some_blueprint"))

    installation.assert_file_written(
        "config.yml",
        {
            "version": 2,
            "inventory_database": "some_blueprint",
            "log_level": "INFO",
            "num_threads": 10,
            "workspace_start_path": "/",
        },
    )


@dataclass
class SomeConfig:
    version: str


def test_filename_inference() -> None:
    installation = MockInstallation()

    installation.save(SomeConfig("0.1.2"))

    installation.assert_file_written("some-config.json", {"version": "0.1.2"})


@dataclass
class EvolvedConfig:
    __file__ = "config.yml"
    __version__ = 3

    initial: int
    added_in_v1: int
    added_in_v2: int

    @staticmethod
    def v1_migrate(raw: dict) -> dict:
        raw["added_in_v1"] = 111
        raw["version"] = 2
        return raw

    @staticmethod
    def v2_migrate(raw: dict) -> dict:
        raw["added_in_v2"] = 222
        raw["version"] = 3
        return raw


def test_migrations_on_load() -> None:
    installation = MockInstallation({"config.yml": {"initial": 999}})

    cfg = installation.load(EvolvedConfig)

    assert 999 == cfg.initial
    assert 111 == cfg.added_in_v1
    assert 222 == cfg.added_in_v2


@dataclass
class BrokenConfig:
    __file__ = "config.yml"
    __version__ = 3

    initial: int
    added_in_v1: int
    added_in_v2: int

    @staticmethod
    def v1_migrate(raw: dict) -> dict:
        raw["added_in_v1"] = 111
        raw["version"] = 2
        return {}


def test_migrations_broken() -> None:
    installation = MockInstallation({"config.yml": {"initial": 999}})

    with pytest.raises(IllegalState):
        installation.load(BrokenConfig)


def test_enable_files_in_repos() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    installation = Installation(ws, "ucx")
    ws.workspace_conf.set_status = MagicMock()

    # enableWorkspaceFilesystem is true
    ws.workspace_conf.get_status.return_value = {"enableWorkspaceFilesystem": "true"}
    installation._enable_files_in_repos()
    ws.workspace_conf.set_status.assert_not_called()

    # enableWorkspaceFilesystem is false
    ws.workspace_conf.get_status.return_value = {"enableWorkspaceFilesystem": "false"}
    installation._enable_files_in_repos()
    ws.workspace_conf.set_status.assert_called_once()
    ws.workspace_conf.set_status.assert_called_with({"enableWorkspaceFilesystem": "true"})


def test_upload_feature_disabled_failure() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.upload.side_effect = [NotFound(error_code="FEATURE_DISABLED"), None]
    installation = Installation(ws, "blueprint")

    installation.save(WorkspaceConfig(inventory_database="some_blueprint"))

    ws.workspace.mkdirs.assert_called_with("/Users/foo/.blueprint")


class SomePolicy:
    def __init__(self, a, b):
        self._a = a
        self._b = b

    def as_dict(self):
        return {"a": self._a, "b": self._b}

    @classmethod
    def from_dict(cls, raw):
        return cls(raw.get("a"), raw.get("b"))

    def __eq__(self, o):
        assert isinstance(o, SomePolicy)
        return self._a == o._a and self._b == o._b


def test_as_dict_serde() -> None:
    installation = MockInstallation()

    policy = SomePolicy(1, 2)
    installation.save(policy, filename="backups/policy-123.json")

    installation.assert_file_written("backups/policy-123.json", {"a": 1, "b": 2})

    load = installation.load(SomePolicy, filename="backups/policy-123.json")
    assert load == policy


@dataclass
class Policy:
    policy_id: str
    name: str

    def as_dict(self) -> dict:
        return {"policy_id": self.policy_id, "name": self.name}


def test_data_class() -> None:
    installation = MockInstallation()
    policy = Policy("123", "foo")
    installation.save(policy, filename="backups/policy-test.json")
    installation.assert_file_written("backups/policy-test.json", {"policy_id": "123", "name": "foo"})
    load = installation.load(Policy, filename="backups/policy-test.json")
    assert load == policy


@dataclass
class ComplexClass:
    name: str
    spark_conf: typing.Dict[str, str]
    policies: typing.List[Policy] | None = None
    policies_map: typing.Dict[str, Policy] | None = None
    CONST: typing.ClassVar[str] = "CONST"


def test_load_complex_data_class() -> None:
    installation = MockInstallation()
    complex_class = ComplexClass("test", {"key": "value"}, [Policy("123", "foo")], {"123": Policy("123", "foo")})
    installation.save(complex_class, filename="backups/complex-class.json")
    installation.assert_file_written(
        "backups/complex-class.json",
        {
            "name": "test",
            "spark_conf": {"key": "value"},
            "policies": [{"policy_id": "123", "name": "foo"}],
            "policies_map": {"123": {"name": "foo", "policy_id": "123"}},
        },
    )
    load = installation.load(ComplexClass, filename="backups/complex-class.json")
    assert load == complex_class


def test_load_empty_data_class() -> None:
    installation = MockInstallation()
    complex_class = ComplexClass("test", {"key": "value"}, None, None)
    installation.save(complex_class, filename="backups/complex-class.json")
    installation.assert_file_written(
        "backups/complex-class.json",
        {
            "name": "test",
            "spark_conf": {"key": "value"},
        },
    )
    load = installation.load(ComplexClass, filename="backups/complex-class.json")
    assert load == complex_class


def test_assert_file_uploaded() -> None:
    installation = MockInstallation()
    installation.upload("foo", b"bar")
    installation.assert_file_uploaded("foo", b"bar")


def test_generic_dict_str() -> None:
    @dataclass
    class SampleClass:
        field: dict[str, str]

    installation = MockInstallation()
    saved = SampleClass(field={"a": "b", "b": "c"})
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert isinstance(loaded.field["a"], str)
    assert loaded == saved


def test_generic_dict_int() -> None:
    @dataclass
    class SampleClass:
        field: dict[str, int]

    installation = MockInstallation()
    saved = SampleClass(field={"a": 1, "b": 1})
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert isinstance(loaded.field["a"], int)
    assert loaded == saved


def test_generic_dict_float() -> None:
    @dataclass
    class SampleClass:
        field: dict[str, float]

    installation = MockInstallation()
    saved = SampleClass(field={"a": 1.1, "b": 1.2})
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert isinstance(loaded.field["a"], float)
    assert loaded == saved


def test_generic_dict_list() -> None:
    @dataclass
    class SampleClass:
        field: dict[str, list[str]]

    installation = MockInstallation()
    saved = SampleClass(field={"a": ["x", "y"], "b": []})
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved


def test_generic_dict_json_value() -> None:
    @dataclass
    class SampleClass:
        field: dict[str, JsonValue]

    installation = MockInstallation()

    json_like: dict[str, JsonValue] = {"a": ["x", "y"], "b": [], "c": 3, "d": True, "e": {"a": "b"}, "f": 0.1}
    saved = SampleClass(field=json_like)
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert isinstance(loaded.field["c"], int)
    assert isinstance(loaded.field["f"], float)
    assert loaded == saved


def test_generic_list_str() -> None:
    @dataclass
    class SampleClass:
        field: list[str]

    installation = MockInstallation()
    saved = SampleClass(field=["a", "b"])
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved


def test_generic_list_int() -> None:
    @dataclass
    class SampleClass:
        field: list[int]

    installation = MockInstallation()
    saved = SampleClass(field=[1, 2, 3])
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved


def test_generic_list_float() -> None:
    @dataclass
    class SampleClass:
        field: list[float]

    installation = MockInstallation()
    saved = SampleClass(field=[1.1, 1.2])
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved


def test_generic_list_list() -> None:
    @dataclass
    class SampleClass:
        field: list[list[str]]

    installation = MockInstallation()
    saved = SampleClass(field=[["x", "y"], []])
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved


def test_generic_list_json() -> None:
    @dataclass
    class SampleClass:
        field: list[JsonValue]

    installation = MockInstallation()
    json_like: list[JsonValue] = [
        ["x", "y"],
        [],
        3,
        True,
        {"a": "b"},
        0.1,
    ]
    saved = SampleClass(field=json_like)
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved


def test_bool_in_union() -> None:
    @dataclass
    class SampleClass:
        field: dict[str, bool | str]

    installation = MockInstallation()
    saved = SampleClass(field={"a": "b"})
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved


def test_complex_union() -> None:
    @dataclass
    class SampleClass:
        field: dict[str, JsonValue]

    installation = MockInstallation()
    saved = SampleClass(field={"a": "b"})
    installation.save(saved, filename="backups/SampleClass.json")


# Alternative union describing the JSON bounds that we support.
JSONValueAlt: TypeAlias = dict[str, "JSONValueAlt"] | list["JSONValueAlt"] | str | float | int | bool | None


def test_complex_union_alt() -> None:
    @dataclass
    class SampleClass:
        field: dict[str, JSONValueAlt]

    installation = MockInstallation()
    saved = SampleClass(field={"a": "b"})
    installation.save(saved, filename="backups/SampleClass.json")

    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved


def test_raw_list_deprecation() -> None:
    @dataclass
    class SampleClass:
        field: list

    installation = MockInstallation()
    saved = SampleClass(field=[1, 2, 3])
    with pytest.warns(DeprecationWarning, match="Raw list serialization is deprecated"):
        installation.save(saved, filename="backups/SampleClass.json")

    # Loading raw (untyped) lists never worked, so there's no need for deprecation: it now fails with instructions.
    with pytest.raises(SerdeError, match=re.escape("field: raw list encountered; use list[type] instead: [1, 2, 3]")):
        installation.load(SampleClass, filename="backups/SampleClass.json")

    @dataclass
    class SampleClassFixed:
        field: list[int]

    loaded = installation.load(SampleClassFixed, filename="backups/SampleClass.json")
    assert dataclasses.asdict(loaded) == dataclasses.asdict(saved)


def test_raw_dict_deprecation() -> None:
    @dataclass
    class SampleClass:
        field: dict

    installation = MockInstallation()
    saved = SampleClass(field={"a": 1, "b": 2, "c": 3})
    with pytest.warns(DeprecationWarning, match="Raw dict serialization is deprecated"):
        installation.save(saved, filename="backups/SampleClass.json")

    # Loading raw (untyped) lists never worked, so there's no need for deprecation: it now fails with instructions.
    with pytest.raises(
        SerdeError, match=re.escape("field: raw dict encountered; use dict[str,type] instead: {'a': 1, 'b': 2, 'c': 3}")
    ):
        installation.load(SampleClass, filename="backups/SampleClass.json")

    @dataclass
    class SampleClassFixed:
        field: dict[str, int]

    loaded = installation.load(SampleClassFixed, filename="backups/SampleClass.json")
    assert dataclasses.asdict(loaded) == dataclasses.asdict(saved)


def test_loading_dict_coercion_special_case() -> None:
    """Earlier versions of the unmarshalling code would sometimes initialize a dataclass field with its default value if:
        - The saved value was falsey;
        - THe saved value could not be coerced to the expected type.

    In particular the UCX code that loads existing dashboards during installation does not expect SerdeError to be
    raised.
    """

    @dataclass
    class OldSampleClass:
        field: dict[str, int] = dataclasses.field(default_factory=dict)

    installation = MockInstallation({"something.json": {"field": "not_a_dict"}})
    loaded = installation.load(OldSampleClass, filename="something.json")

    assert loaded == OldSampleClass(field={})


def test_loading_value_coercion_to_str() -> None:
    """When the stored values don't match the hinted type, behaviour is to coerce if possible."""

    @dataclass
    class SampleClass:
        field_str: str
        field_bool: str
        field_int: str
        field_float: str

    installation = MockInstallation(
        {"something.json": {"field_str": "str", "field_bool": True, "field_int": 1, "field_float": 1.1}}
    )
    loaded = installation.load(SampleClass, filename="something.json")
    assert loaded == SampleClass(field_str="str", field_bool="True", field_int="1", field_float="1.1")


def test_loading_value_coercion_from_str() -> None:
    """When the underlying values don't match the hinted type, pre-existing behaviour is to coerce if possible."""

    @dataclass
    class SampleClass:
        field_dict: dict[str, int]
        field_bool: bool
        field_int: int
        field_floats: dict[str, float]

    raw_data: dict[str, typing.Any] = {
        "field_dict": {"a": "1", "b": "2"},
        "field_bool": "tRuE",
        "field_int": "42",
        "field_floats": {"x": "3.14", "y": 2},
    }
    expected = SampleClass(
        field_dict={"a": 1, "b": 2}, field_bool=True, field_int=42, field_floats={"x": 3.14, "y": 2.0}
    )

    installation = MockInstallation({"something.json": raw_data})
    loaded = installation.load(SampleClass, filename="something.json")
    assert loaded == expected


def test_forward_referencing_class() -> None:
    """Test that a class with forward-referenced fields. This simulates the behavior of future annotations."""

    @dataclass
    class ForwardReferencingClass:
        field_str: "str" = "foo"
        field_int: "int" = 20
        field_bool: "bool" = False
        field_float: "float" = 2.3
        field_dict: "dict[str, int]" = dataclasses.field(default_factory=dict)
        field_list: "list[str]" = dataclasses.field(default_factory=list)
        field_optional: "str | None" = None
        field_json: "JsonValue" = None

    instance = ForwardReferencingClass(field_dict={"a": 1, "b": 2}, field_list=["x", "y"], field_json={"key": "value"})

    installation = MockInstallation()
    installation.save(instance, filename="saved.yml")

    loaded = installation.load(ForwardReferencingClass, filename="saved.yml")
    assert instance == loaded

def test_bool_attribute() -> None:

    @dataclass
    class MissingAttributeClass:
        __file__ = "config.yml"
        __version__ = 3
        skip_validation: bool = True
        sdk_config: JsonValue = None

    instance = MissingAttributeClass(False, {"warehouse_id": "8xc123456"})
    installation = MockInstallation()
    installation.save(instance)

    loaded = installation.load(MissingAttributeClass)
    assert instance == loaded
