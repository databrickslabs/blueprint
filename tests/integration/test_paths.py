import codecs
from datetime import datetime
from pathlib import Path

import pytest
from databricks.sdk.errors import BadRequest, ResourceAlreadyExists

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
def test_stat(ws, make_random, cls):
    now = datetime.now().timestamp()
    name = make_random()
    wsp = cls(ws, f"~/{name}/a/b/c")
    with_user = wsp.expanduser()
    with_user.mkdir(parents=True)

    hello_txt = with_user / "hello.txt"
    hello_txt.write_text("Hello, World!")
    if cls is WorkspacePath: # DBFSPath has no st_ctime
        assert hello_txt.stat().st_ctime >= now
    assert hello_txt.stat().st_mtime >= now


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
def test_rename_file(ws, make_random, cls):
    name = make_random()
    tmp_dir = cls(ws, f"~/{name}").expanduser()
    tmp_dir.mkdir()
    try:
        # Test renaming a file when the target doesn't exist.
        src_file = tmp_dir / "src.txt"
        src_file.write_text("Some content")
        dst_file = src_file.rename(src_file.with_name("dst.txt"))
        expected_file = tmp_dir / "dst.txt"
        assert dst_file == expected_file and expected_file.is_file()

        # Test renaming a file when the target already exists.
        exists_file = tmp_dir / "already-exists.txt"
        exists_file.write_text("Existing file.")
        with pytest.raises(ResourceAlreadyExists):
            _ = dst_file.rename(exists_file)
        assert expected_file.exists() and expected_file.is_file()  # Check it's still there.
    finally:
        tmp_dir.rmdir(recursive=True)


def test_rename_directory(ws, make_random):
    # The Workspace client doesn't currently support renaming directories so we only test DBFS.
    name = make_random()
    tmp_dir = DBFSPath(ws, f"~/{name}").expanduser()
    tmp_dir.mkdir()
    try:
        # Test renaming a directory (with content) when the target doesn't exist.
        src_dir = tmp_dir / "src-dir"
        src_dir.mkdir()
        (src_dir / "content.txt").write_text("Source content.")
        dst_dir = src_dir.rename(src_dir.with_name("dst-dir"))
        expected_dir = tmp_dir / "dst-dir"
        assert dst_dir == expected_dir and expected_dir.is_dir() and (expected_dir / "content.txt").is_file()

        # Test renaming a directory (with content) when the target already exists.
        exists_dir = tmp_dir / "existing-dir"
        exists_dir.mkdir()
        with pytest.raises(ResourceAlreadyExists):
            _ = dst_dir.rename(exists_dir)
        assert expected_dir.exists() and expected_dir.is_dir()  # Check it's still there.
    finally:
        tmp_dir.rmdir(recursive=True)


@pytest.mark.parametrize("cls", DATABRICKS_PATHLIKE)
def test_replace_file(ws, make_random, cls):
    name = make_random()
    tmp_dir = cls(ws, f"~/{name}").expanduser()
    tmp_dir.mkdir()
    try:
        # Test replacing a file when the target doesn't exist.
        src_file = tmp_dir / "src.txt"
        src_file.write_text("Some content")
        dst_file = src_file.replace(src_file.with_name("dst.txt"))
        expected_file = tmp_dir / "dst.txt"
        assert dst_file == expected_file and expected_file.is_file()

        # Test replacing a file when the target already exists.
        exists_file = tmp_dir / "already-exists.txt"
        exists_file.write_text("Existing file.")
        replaced_file = dst_file.replace(exists_file)
        assert replaced_file.is_file() and replaced_file.read_text() == "Some content"
    finally:
        tmp_dir.rmdir(recursive=True)


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


@pytest.mark.parametrize(
    "bom, encoding",
    [
        (codecs.BOM_UTF8, "utf-8"),
        (codecs.BOM_UTF16_LE, "utf-16-le"),
        (codecs.BOM_UTF16_BE, "utf-16-be"),
        (codecs.BOM_UTF32_LE, "utf-32-le"),
        (codecs.BOM_UTF32_BE, "utf-32-be"),
    ],
)
def test_correctly_encodes_and_decodes_file_with_bom(bom, encoding, ws, make_directory):
    # Can't test notebooks because the server changes the uploaded data
    folder = WorkspacePath(ws, make_directory())
    file_path = folder / f"some_file_{encoding}.py"
    data = bom + "a = 12".encode(encoding)
    file_path.write_bytes(data)
    text = file_path.read_text()
    assert text == "a = 12"
