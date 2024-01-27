import io
from dataclasses import dataclass
from unittest.mock import create_autospec

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
    MockInstallation,
)


def test_current_not_found():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.get_status.side_effect = NotFound(...)

    with pytest.raises(NotFound, match="Application not installed: blueprint"):
        Installation.current(ws, "blueprint")


def test_current_not_found_assume_user():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.get_status.side_effect = NotFound(...)

    installation = Installation.current(ws, "blueprint", assume_user=True)
    assert "/Users/foo/.blueprint" == installation.install_folder()


def test_current_found_user():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.get_status.side_effect = None

    installation = Installation.current(ws, "blueprint")
    assert "/Users/foo/.blueprint" == installation.install_folder()


def test_current_found_root():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.get_status.side_effect = [NotFound(...), None]

    installation = Installation.current(ws, "blueprint")
    assert "/Applications/blueprint" == installation.install_folder()


def test_existing_not_found():
    ws = create_autospec(WorkspaceClient)
    ws.users.list.return_value = [iam.User(user_name="foo")]
    ws.workspace.get_status.side_effect = NotFound(...)

    existing = Installation.existing(ws, "blueprint")
    assert [] == existing

    ws.workspace.get_status.assert_any_call("/Applications/blueprint")
    ws.workspace.get_status.assert_any_call("/Users/foo/.blueprint")
    assert 2 == ws.workspace.get_status.call_count


def test_existing_found_root():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.side_effect = None

    existing = Installation.existing(ws, "blueprint")
    assert "/Applications/blueprint" == existing[0].install_folder()


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


def test_save_typed_file():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    state = Installation(ws, "blueprint")

    target = state.save(
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
                "$version": 2,
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


def test_creates_missing_folders():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.upload.side_effect = [NotFound(...), None]
    state = Installation(ws, "blueprint")

    state.save(WorkspaceConfig(inventory_database="some_blueprint"))

    ws.workspace.mkdirs.assert_called_with("/Users/foo/.blueprint")


def test_upload_dbfs():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    state = Installation(ws, "blueprint")

    target = state.upload_dbfs("wheels/foo.whl", b"abc")
    assert "/Users/foo/.blueprint/wheels/foo.whl" == target


def test_upload_dbfs_mkdirs():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.dbfs.upload.side_effect = [NotFound(...), None]
    state = Installation(ws, "blueprint")

    target = state.upload_dbfs("wheels/foo.whl", b"abc")
    assert "/Users/foo/.blueprint/wheels/foo.whl" == target

    ws.dbfs.mkdirs.assert_called_with("/Users/foo/.blueprint/wheels")


def test_save_typed_file_array_csv():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    state = Installation(ws, "blueprint")

    state.save(
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


def test_load_typed_file():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value = io.StringIO(
        yaml.dump(
            {
                "$version": 2,
                "num_threads": 20,
                "inventory_database": "some_blueprint",
                "connect": {"host": "https://foo", "token": "bar"},
            }
        )
    )
    state = Installation(ws, "blueprint")

    cfg = state.load(WorkspaceConfig)

    assert 20 == cfg.num_threads
    assert "/" == cfg.workspace_start_path


def test_load_csv_file():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value = io.StringIO(
        "\n".join(["workspace_id,workspace_name", "1234,first", "1235,second"])
    )
    state = Installation(ws, "blueprint")

    workspaces = state.load(list[Workspace], filename="workspaces.csv")

    assert 2 == len(workspaces)
    assert "first" == workspaces[0].workspace_name
    assert 1235 == workspaces[1].workspace_id


@pytest.mark.parametrize("ext", ["json", "csv"])
def test_load_typed_list_file(ext):
    state = MockInstallation(
        {
            f"workspaces.{ext}": [
                {"workspace_id": 1234, "workspace_name": "first"},
                {"workspace_id": 1235, "workspace_name": "second"},
            ]
        }
    )

    workspaces = state.load(list[Workspace], filename=f"workspaces.{ext}")

    assert 2 == len(workspaces)
    assert "first" == workspaces[0].workspace_name
    assert 1235 == workspaces[1].workspace_id


def test_save_typed_file_array_json():
    state = MockInstallation()

    state.save(
        [
            Workspace(workspace_id=1234, workspace_name="first"),
            Workspace(workspace_id=1235, workspace_name="second"),
        ],
        filename="workspaces.json",
    )

    state.assert_file_written(
        "workspaces.json",
        [{"workspace_id": 1234, "workspace_name": "first"}, {"workspace_id": 1235, "workspace_name": "second"}],
    )


def test_mock_save_typed_file():
    state = MockInstallation()

    state.save(WorkspaceConfig(inventory_database="some_blueprint"))

    state.assert_file_written(
        "config.yml",
        {
            "$version": 2,
            "inventory_database": "some_blueprint",
            "log_level": "INFO",
            "num_threads": 10,
            "workspace_start_path": "/",
        },
    )


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
        raw["$version"] = 2
        return raw

    @staticmethod
    def v2_migrate(raw: dict) -> dict:
        raw["added_in_v2"] = 222
        raw["$version"] = 3
        return raw


def test_migrations_on_load():
    state = MockInstallation({"config.yml": {"initial": 999}})

    cfg = state.load(EvolvedConfig)

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
        raw["$version"] = 2
        return {}


def test_migrations_broken():
    state = MockInstallation({"config.yml": {"initial": 999}})

    with pytest.raises(IllegalState):
        state.load(BrokenConfig)
