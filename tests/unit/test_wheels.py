import os
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.blueprint.__about__ import __version__
from databricks.labs.blueprint.entrypoint import is_in_debug
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo, Wheels


def test_build_and_upload_wheel():
    ws = create_autospec(WorkspaceClient)
    state = create_autospec(InstallState)
    state.product.return_value = "blueprint"
    state.install_folder.return_value = "~/.blueprint"
    product_info = ProductInfo(__file__)

    wheels = Wheels(ws, state, product_info)
    with wheels:
        assert os.path.exists(wheels._local_wheel)

        remote_on_wsfs = wheels.upload_to_wsfs()
        ws.workspace.mkdirs.assert_called_once_with("~/.blueprint/wheels")
        ws.workspace.upload.assert_called_once()

        call = ws.workspace.upload.mock_calls[0]
        path = call.args[0]
        assert remote_on_wsfs == path
        assert path.startswith("~/.blueprint/wheels/databricks_labs_blueprint-")
        assert ImportFormat.AUTO == call.kwargs["format"]
        assert call.kwargs["overwrite"]

        wheels.upload_to_dbfs()
        ws.dbfs.mkdirs.assert_called_once_with("~/.blueprint/wheels")
        ws.dbfs.upload.assert_called_once()
    assert not os.path.exists(wheels._local_wheel)


def test_unreleased_version(tmp_path):
    if not is_in_debug():
        pytest.skip("fails without `git fetch --prune --unshallow` configured")
    product_info = ProductInfo(__file__)
    assert not __version__ == product_info.version()
    assert __version__ == product_info.released_version()
    assert product_info.is_unreleased_version()
    assert product_info.is_git_checkout()


def test_released_version(tmp_path):
    ws = create_autospec(WorkspaceClient)
    state = create_autospec(InstallState)

    working_copy = Wheels(ws, state, ProductInfo(__file__))._copy_root_to(tmp_path)
    product_info = ProductInfo(working_copy)

    assert __version__ == product_info.version()
    assert not product_info.is_unreleased_version()
    assert not product_info.is_git_checkout()
