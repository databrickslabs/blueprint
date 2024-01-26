import io
from dataclasses import dataclass
from unittest.mock import create_autospec

import pytest
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import NotFound
from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.blueprint.installation import (
    IllegalState,
    Installation,
    MockInstallation,
)


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

    state.save(WorkspaceConfig(inventory_database="some_blueprint"))

    ws.workspace.upload.assert_called_with(
        "/Users/foo/.blueprint/config.yml",
        yaml.dump(
            {
                "$version": 2,
                "num_threads": 10,
                "inventory_database": "some_blueprint",
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


def test_migrations_on_load():
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

    state = MockInstallation({"config.yml": {"initial": 999}})

    cfg = state.load(EvolvedConfig)

    assert 999 == cfg.initial
    assert 111 == cfg.added_in_v1
    assert 222 == cfg.added_in_v2


def test_migrations_broken():
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

    state = MockInstallation({"config.yml": {"initial": 999}})

    with pytest.raises(IllegalState):
        state.load(BrokenConfig)
