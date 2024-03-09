import os
import re
from pathlib import Path

import pytest
from databricks.sdk.service.compute import Language

from databricks.labs.blueprint.__about__ import __version__
from databricks.labs.blueprint.entrypoint import is_in_debug
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.wheels import (
    ProductInfo,
    SingleSourceVersionError,
    WheelsV2,
)


def test_build_and_upload_wheel():
    installation = MockInstallation()
    product_info = ProductInfo.from_class(MockInstallation)

    wheels = WheelsV2(installation, product_info)
    with wheels:
        assert os.path.exists(wheels._local_wheel)

        remote_on_wsfs = wheels.upload_to_wsfs()
        installation.assert_file_uploaded(re.compile("wheels/databricks_labs_blueprint-*"))
        installation.assert_file_written(
            "version.json",
            {
                "version": product_info.version(),
                "wheel": remote_on_wsfs,
                "date": ...,
            },
        )

        wheels.upload_to_dbfs()
        installation.assert_file_dbfs_uploaded(re.compile("wheels/databricks_labs_blueprint-*"))
    assert not os.path.exists(wheels._local_wheel)


def test_unreleased_version(tmp_path):
    if not is_in_debug():
        pytest.skip("fails without `git fetch --prune --unshallow` configured")
    product_info = ProductInfo.from_class(MockInstallation)
    assert not __version__ == product_info.version()
    assert __version__ == product_info.released_version()
    assert product_info.is_unreleased_version()
    assert product_info.is_git_checkout()


def test_released_version(tmp_path):
    installation = MockInstallation()
    info = ProductInfo.from_class(MockInstallation)
    working_copy = WheelsV2(installation, info)._copy_root_to(tmp_path)
    product_info = ProductInfo(working_copy / "src/databricks/labs/blueprint/cli.py")

    assert product_info.product_name() == "blueprint"
    assert __version__ == product_info.version()
    assert not product_info.is_unreleased_version()
    assert not product_info.is_git_checkout()
    assert product_info.unreleased_version() == product_info.released_version()


def test_determines_sdk_version():
    from databricks.sdk.version import __version__ as sdk_version

    sdk_info = ProductInfo.from_class(Language)
    released_version = sdk_info.released_version()
    assert sdk_version == released_version


def test_no_version_marker_found():
    with pytest.raises(SingleSourceVersionError):
        ProductInfo(__file__)


def test_marker_without_version_variable():
    no_version_fixture = Path(__file__).parent / "fixtures/some/__init__.py"
    with pytest.raises(SingleSourceVersionError):
        ProductInfo(no_version_fixture.as_posix())


def test_product_info_for_testing_product_names_are_different():
    a = ProductInfo.from_class(MockInstallation)
    b = ProductInfo.for_testing(MockInstallation)
    c = ProductInfo.for_testing(MockInstallation)
    assert a.product_name() != b.product_name()
    assert b.product_name() != c.product_name()
    assert a.product_name() != c.product_name()