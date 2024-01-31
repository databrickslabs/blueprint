import io
import json
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.blueprint.installation import IllegalState, MockInstallation
from databricks.labs.blueprint.installer import InstallState


def test_from_installation():
    installation = MockInstallation()
    state = InstallState.from_installation(installation)
    assert "~/mock" == state.install_folder()


def test_install_folder():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    state = InstallState(ws, "blueprint")
    assert "/Users/foo/.blueprint" == state.install_folder()


def test_jobs_state():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value = io.StringIO('{"$version":1, "resources": {"jobs": {"foo": 123}}}')

    state = InstallState(ws, "blueprint")

    assert {"foo": "123"} == state.jobs
    assert {} == state.dashboards
    ws.workspace.download.assert_called_with("/Users/foo/.blueprint/state.json")


def test_invalid_config_version():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value = io.StringIO('{"$version":9, "resources": {"jobs": [1,2,3]}}')

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
    ws.workspace.download.return_value = io.StringIO('{"$versio...')

    state = InstallState(ws, "blueprint")

    assert {} == state.jobs


def test_state_overwrite_existing():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"
    ws.workspace.download.return_value = io.StringIO('{"$version":1, "resources": {"sql": {"a": "b"}}}')

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
