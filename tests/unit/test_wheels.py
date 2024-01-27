import os
import re

import pytest

from databricks.labs.blueprint.__about__ import __version__
from databricks.labs.blueprint.entrypoint import is_in_debug
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2


def test_build_and_upload_wheel():
    installation = MockInstallation()
    product_info = ProductInfo(__file__)

    wheels = WheelsV2(installation, product_info)
    with wheels:
        assert os.path.exists(wheels._local_wheel)

        remote_on_wsfs = wheels.upload_to_wsfs()
        installation.assert_file_uploaded(re.compile("wheels/databricks_labs_blueprint-*"))
        installation.assert_file_written("version.json", {"version": product_info.version(), "wheel": remote_on_wsfs})

        wheels.upload_to_dbfs()
        installation.assert_file_dbfs_uploaded(re.compile("wheels/databricks_labs_blueprint-*"))
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
    installation = MockInstallation()
    working_copy = WheelsV2(installation, ProductInfo(__file__))._copy_root_to(tmp_path)
    product_info = ProductInfo(working_copy)

    assert __version__ == product_info.version()
    assert not product_info.is_unreleased_version()
    assert not product_info.is_git_checkout()
