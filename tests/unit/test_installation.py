import io
from dataclasses import dataclass
from unittest.mock import create_autospec

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.blueprint.installation import Installation, MockInstallation


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


def test_load_typed_file():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value = io.StringIO(
        yaml.dump(
            {
                "$version": 2,
                "num_threads": 20,
                "inventory_database": "some_blueprint",
            }
        )
    )
    state = Installation(ws, "blueprint")

    cfg = state.load(WorkspaceConfig)

    assert 20 == cfg.num_threads


def test_save_typed_file_array():
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
