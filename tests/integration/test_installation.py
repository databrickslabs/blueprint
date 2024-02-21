import random
from dataclasses import dataclass

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.provisioning import Workspace

from databricks.labs.blueprint.installation import Installation


def test_install_folder(ws):
    installation = Installation(ws, "blueprint")

    assert installation.install_folder() == f"/Users/{ws.current_user.me().user_name}/.blueprint"


def test_install_folder_custom(ws):
    installation = Installation(ws, "blueprint", install_folder="/custom/folder")

    assert installation.install_folder() == "/custom/folder"


def test_flaky():

    assert 1 == random.choice([1, 2])


@pytest.mark.xfail(raises=PermissionDenied)
def test_detect_global(ws, make_random):
    product = make_random(4)
    Installation(ws, product, install_folder=f"/Applications/{product}").upload("some", b"...")

    current = Installation.current(ws, product)

    assert current.install_folder() == f"/Applications/{product}_"


# integration tests are running from lower-privileged environment
@pytest.mark.xfail(raises=PermissionDenied)
def test_existing(ws, make_random):
    product = make_random(4)

    global_install = Installation(ws, product, install_folder=f"/Applications/{product}")
    global_install.upload("some", b"...")

    user_install = Installation(ws, product)
    user_install.upload("some2", b"...")

    existing = Installation.existing(ws, product)
    assert set(existing) == {global_install, user_install}


@dataclass
class MyClass:
    field1: str
    field2: str


def test_dataclass(new_installation):
    obj = MyClass("value1", "value2")
    new_installation.save(obj)

    # Verify that the object was saved correctly
    loaded_obj = new_installation.load(MyClass)
    assert loaded_obj == obj


def test_csv(new_installation):
    new_installation.save(
        [
            Workspace(workspace_id=1234, workspace_name="first"),
            Workspace(workspace_id=1235, workspace_name="second"),
        ],
        filename="workspaces.csv",
    )

    loaded = new_installation.load(list[Workspace], filename="workspaces.csv")
    assert len(loaded) == 2


@pytest.mark.parametrize(
    "ext,magic",
    [
        ("py", "# Databricks notebook source"),
        ("scala", "// Databricks notebook source"),
        ("sql", "-- Databricks notebook source"),
    ],
)
def test_uploading_notebooks_get_correct_urls(ext, magic, new_installation):
    remote_path = new_installation.upload(f"foo.{ext}", f"{magic}\nprint(1)".encode("utf8"))
    assert f"{new_installation.install_folder()}/foo" == remote_path
