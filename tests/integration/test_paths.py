from pathlib import Path

import pytest
from databricks.sdk.errors import BadRequest

from databricks.labs.blueprint.paths import DBFSPath, WorkspacePath

# Currently: DBFSPath, WorkspacePath, later: VolumePath
DATABRICKS_PATHLIKE = [DBFSPath, WorkspacePath]


@pytest.mark.parametrize("cls", DATABRICKS_PATHLIKE)
def test_exists(ws, cls):
    wsp = cls(ws, "/Users/foo/bar/baz")
    assert not wsp.exists()


@pytest.mark.parametrize("cls", DATABRICKS_PATHLIKE)
def test_mkdirs(ws, make_random, cls):
    name = make_random()
    wsp = cls(ws, f"~/{name}/foo/bar/baz")
    assert not wsp.is_absolute()

    with pytest.raises(NotImplementedError):
        wsp.absolute()

    with_user = wsp.expanduser()
    with_user.mkdir()

    home = cls(ws, "~").expanduser()
    relative_name = with_user.relative_to(home)
    assert relative_name.as_posix() == f"{name}/foo/bar/baz"

    assert with_user.is_absolute()
    assert with_user.absolute() == with_user

    user_name = ws.current_user.me().user_name
    wsp_check = cls(ws, f"/Users/{user_name}/{name}/foo/bar/baz")
    assert wsp_check.is_dir()

    with pytest.raises(expected_exception=(BadRequest, OSError)):
        wsp_check.parent.rmdir()
    wsp_check.parent.rmdir(recursive=True)

    assert not wsp_check.exists()


@pytest.mark.parametrize("cls", DATABRICKS_PATHLIKE)
def test_open_text_io(ws, make_random, cls):
    name = make_random()
    wsp = cls(ws, f"~/{name}/a/b/c")
    with_user = wsp.expanduser()
    with_user.mkdir(parents=True)

    hello_txt = with_user / "hello.txt"
    hello_txt.write_text("Hello, World!")
    assert hello_txt.read_text() == "Hello, World!"

    files = list(with_user.glob("**/*.txt"))
    assert len(files) == 1
    assert hello_txt == files[0]
    assert files[0].name == "hello.txt"

    with_user.joinpath("hello.txt").unlink()

    assert not hello_txt.exists()


@pytest.mark.parametrize("cls", DATABRICKS_PATHLIKE)
def test_unlink(ws, make_random, cls):
    name = make_random()
    tmp_dir = cls(ws, f"~/{name}").expanduser()
    tmp_dir.mkdir()
    try:
        # Check unlink() interactions with a file that exists.
        some_file = tmp_dir / "some-file.txt"
        some_file.write_text("Some text")
        assert some_file.exists() and some_file.is_file()
        some_file.unlink()
        assert not some_file.exists()

        # And now the interactions with a missing file.
        missing_file = tmp_dir / "missing-file.txt"
        missing_file.unlink(missing_ok=True)
        with pytest.raises(FileNotFoundError):
            missing_file.unlink(missing_ok=False)
    finally:
        tmp_dir.rmdir(recursive=True)


@pytest.mark.parametrize("cls", DATABRICKS_PATHLIKE)
def test_open_binary_io(ws, make_random, cls):
    name = make_random()
    wsp = cls(ws, f"~/{name}")
    with_user = wsp.expanduser()
    with_user.mkdir(parents=True)

    hello_bin = with_user.joinpath("hello.bin")
    hello_bin.write_bytes(b"Hello, World!")

    assert hello_bin.read_bytes() == b"Hello, World!"

    with_user.joinpath("hello.bin").unlink()

    assert not hello_bin.exists()


@pytest.mark.parametrize("cls", DATABRICKS_PATHLIKE)
def test_replace(ws, make_random, cls):
    name = make_random()
    wsp = cls(ws, f"~/{name}")
    with_user = wsp.expanduser()
    with_user.mkdir(parents=True)

    hello_txt = with_user / "hello.txt"
    hello_txt.write_text("Hello, World!")

    hello_txt.replace(with_user / "hello2.txt")

    assert not hello_txt.exists()
    assert (with_user / "hello2.txt").read_text() == "Hello, World!"


def test_workspace_as_fuse(ws):
    wsp = WorkspacePath(ws, "/Users/foo/bar/baz")
    assert Path("/Workspace/Users/foo/bar/baz") == wsp.as_fuse()


def test_dbfs_as_fuse(ws):
    p = DBFSPath(ws, "/Users/foo/bar/baz")
    assert Path("/dbfs/Users/foo/bar/baz") == p.as_fuse()


def test_workspace_as_uri(ws):
    # DBFS is not exposed via browser
    wsp = WorkspacePath(ws, "/Users/foo/bar/baz")
    assert wsp.as_uri() == f"{ws.config.host}#workspace/Users/foo/bar/baz"


def test_file_and_notebook_in_same_folder_with_different_suffixes(ws, make_notebook, make_directory):
    folder = WorkspacePath(ws, make_directory())

    txt_file = folder / "a.txt"
    py_notebook = folder / "b"  # notebooks have no file extension

    make_notebook(path=py_notebook, content="display(spark.range(10))")
    txt_file.write_text("Hello, World!")

    files = {_.name: _ for _ in folder.glob("**/*")}
    assert len(files) == 2

    assert files["a.txt"].suffix == ".txt"
    assert files["b"].suffix == ".py"  # suffix is determined from ObjectInfo
    assert files["b"].read_text() == "# Databricks notebook source\ndisplay(spark.range(10))"
