import json
from dataclasses import dataclass
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.blueprint.installer import IllegalState, InstallState


def test_install_folder():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    state = InstallState(ws, "blueprint")
    assert "/Users/foo/.blueprint" == state.install_folder()


def test_jobs_state():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value.read.return_value = '{"$version":1, "resources": {"jobs": [1,2,3]}}'

    state = InstallState(ws, "blueprint")

    assert [1, 2, 3] == state.jobs
    ws.workspace.download.assert_called_with("/Users/foo/.blueprint/state.json")


def test_invalid_config_version():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value.read.return_value = '{"$version":9, "resources": {"jobs": [1,2,3]}}'

    state = InstallState(ws, "blueprint")

    with pytest.raises(IllegalState):
        _ = state.jobs


def test_state_not_found():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.side_effect = NotFound(...)

    state = InstallState(ws, "blueprint")

    assert {} == state.jobs


def test_state_corrupt():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value.read.return_value = '{"$versio...'

    state = InstallState(ws, "blueprint")

    assert {} == state.jobs


def test_state_overwrite_existing():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value.read.return_value = '{"$version":1, "resources": {"sql": {"a": "b"}}}'

    state = InstallState(ws, "blueprint")
    state.jobs["foo"] = "bar"
    state.save()

    new_state = {"resources": {"sql": {"a": "b"}, "jobs": {"foo": "bar"}}, "$version": 1}
    ws.workspace.upload.assert_called_with(
        "/Users/foo/.blueprint/state.json",
        json.dumps(new_state, indent=2).encode("utf8"),
        format=ImportFormat.AUTO,
        overwrite=True,
    )

@dataclass
class WorkspaceConfig:
    __file__ = 'config.yml'
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
    state = InstallState(ws, "blueprint")

    state.save_typed_file(WorkspaceConfig(
        inventory_database='some_blueprint'
    ))
