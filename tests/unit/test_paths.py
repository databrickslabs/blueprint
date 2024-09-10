import os
from collections.abc import Iterator
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from unittest.mock import create_autospec, patch

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.mixins.workspace import WorkspaceExt
from databricks.sdk.service.files import FileInfo
from databricks.sdk.service.workspace import (
    ImportFormat,
    Language,
    ObjectInfo,
    ObjectType,
)

from databricks.labs.blueprint.paths import DBFSPath, WorkspacePath


def test_empty_init() -> None:
    """Ensure that basic initialization works."""
    ws = create_autospec(WorkspaceClient)

    # Simple initialisation; the empty path is valid.
    WorkspacePath(ws)


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        # Some absolute paths.
        (["/foo/bar"], ("/", ("/", "foo", "bar"))),
        (["/", "foo", "bar"], ("/", ("/", "foo", "bar"))),
        (["/", "foo/bar"], ("/", ("/", "foo", "bar"))),
        (["/", "foo", "bar/baz"], ("/", ("/", "foo", "bar", "baz"))),
        # Some relative paths.
        (["foo/bar"], ("", ("foo", "bar"))),
        (["foo", "bar"], ("", ("foo", "bar"))),
        (["foo", "/bar"], ("/", ("/", "bar"))),
        # Some paths with mixed components of Path types.
        (["/", "foo", PurePosixPath("bar/baz")], ("/", ("/", "foo", "bar", "baz"))),
        (["/", "foo", PureWindowsPath("config.sys")], ("/", ("/", "foo", "config.sys"))),
        (["/", "foo", PurePath("bar")], ("/", ("/", "foo", "bar"))),
        # Some corner cases: empty, root and trailing '/'.
        ([], ("", ())),
        (["/"], ("/", ("/",))),
        (["/foo/"], ("/", ("/", "foo"))),
        # Intermediate '.' are supposed to be dropped during normalization.
        (["/foo/./bar"], ("/", ("/", "foo", "bar"))),
    ],
    ids=lambda param: f"WorkspacePath({param!r})" if isinstance(param, list) else repr(param),
)
def test_init(args: list[str | PurePath], expected: tuple[str, list[str]]) -> None:
    """Ensure that initialization with various combinations of segments works as expected."""
    ws = create_autospec(WorkspaceClient)

    # Run the test.
    p = WorkspacePath(ws, Path(*args))

    # Validate the initialisation results.
    assert (p.drive, p.root, p.parts) == ("", *expected)


def test_init_error() -> None:
    """Ensure that we detect initialisation with non-string or path-like path components."""
    ws = create_autospec(WorkspaceClient)

    expected_msg = "argument should be a str or an os.PathLib object where __fspath__ returns a str, not 'int'"
    with pytest.raises(TypeError, match=expected_msg):
        WorkspacePath(ws, 12)  # type: ignore[arg-type]


def test_equality() -> None:
    """Test that Workspace paths can be compared with each other for equality."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/foo/bar") == WorkspacePath(ws, Path("/", "foo", "bar"))
    assert WorkspacePath(ws, "foo/bar") == WorkspacePath(ws, Path("foo", "bar"))
    assert WorkspacePath(ws, "/foo/bar") != WorkspacePath(ws, Path("foo", "bar"))

    assert WorkspacePath(ws, "/foo/bar") != Path("foo", "bar")
    assert Path("/foo/bar") != WorkspacePath(ws, "/foo/bar")


def test_hash() -> None:
    """Test that equal Workspace paths have the same hash value."""
    ws = create_autospec(WorkspaceClient)

    assert hash(WorkspacePath(ws, "/foo/bar")) == hash(WorkspacePath(ws, Path("/", "foo", "bar")))
    assert hash(WorkspacePath(ws, "foo/bar")) == hash(WorkspacePath(ws, Path("foo", "bar")))


@pytest.mark.parametrize(
    "increasing_paths",
    [
        ("/foo", "/foo/bar", "/foo/baz"),
        ("foo", "foo/bar", "foo/baz"),
    ],
)
def test_comparison(increasing_paths: tuple[str, str, str]) -> None:
    """Test that comparing paths works as expected."""
    ws = create_autospec(WorkspaceClient)

    p1, p2, p3 = (WorkspacePath(ws, p) for p in increasing_paths)
    assert p1 < p2 < p3
    assert p1 <= p2 <= p2 <= p3
    assert p3 > p2 > p1
    assert p3 >= p2 >= p2 > p1


def test_comparison_errors() -> None:
    """Test that comparing Workspace paths with other types yields the error we expect, irrespective of comparison order."""
    ws = create_autospec(WorkspaceClient)

    with pytest.raises(TypeError, match="'<' not supported between instances"):
        _ = WorkspacePath(ws, "foo") < PurePosixPath("foo")
    with pytest.raises(TypeError, match="'>' not supported between instances"):
        _ = WorkspacePath(ws, "foo") > PurePosixPath("foo")
    with pytest.raises(TypeError, match="'<=' not supported between instances"):
        _ = WorkspacePath(ws, "foo") <= PurePosixPath("foo")
    with pytest.raises(TypeError, match="'>=' not supported between instances"):
        _ = WorkspacePath(ws, "foo") >= PurePosixPath("foo")
    with pytest.raises(TypeError, match="'<' not supported between instances"):
        _ = PurePosixPath("foo") < WorkspacePath(ws, "foo")
    with pytest.raises(TypeError, match="'>' not supported between instances"):
        _ = PurePosixPath("foo") > WorkspacePath(ws, "foo")
    with pytest.raises(TypeError, match="'<=' not supported between instances"):
        _ = PurePosixPath("foo") <= WorkspacePath(ws, "foo")
    with pytest.raises(TypeError, match="'>=' not supported between instances"):
        _ = PurePosixPath("foo") >= WorkspacePath(ws, "foo")


def test_drive() -> None:
    """Test that the drive is empty for our paths."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/foo").drive == ""
    assert WorkspacePath(ws, "foo").root == ""


def test_root() -> None:
    """Test that absolute paths have the '/' root and relative paths do not."""
    # More comprehensive tests are part of test_init()
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/foo").root == "/"
    assert WorkspacePath(ws, "foo").root == ""


def test_anchor() -> None:
    """Test that the anchor for absolute paths is '/' and empty for relative paths."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/foo").anchor == "/"
    assert WorkspacePath(ws, "foo").anchor == ""


def test_pathlike_error() -> None:
    """Paths are "path-like" but Workspace ones aren't, so verify that triggers an error."""
    ws = create_autospec(WorkspaceClient)
    p = WorkspacePath(ws, "/some/path")

    with pytest.raises(NotImplementedError, match="WorkspacePath paths are not path-like"):
        _ = os.fspath(p)


def test_name() -> None:
    """Test that the last part of the path is properly noted as the name."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/foo/bar").name == "bar"
    assert WorkspacePath(ws, "/foo/").name == "foo"
    assert WorkspacePath(ws, "/").name == ""
    assert WorkspacePath(ws, Path()).name == ""


def test_parts() -> None:
    """Test that parts returns the anchor and path components."""
    # More comprehensive tests are part of test_init()
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/foo/bar").parts == ("/", "foo", "bar")
    assert WorkspacePath(ws, "/foo/").parts == ("/", "foo")
    assert WorkspacePath(ws, "/").parts == ("/",)
    assert WorkspacePath(ws, "foo/bar").parts == ("foo", "bar")
    assert WorkspacePath(ws, "foo/").parts == ("foo",)


def test_suffix() -> None:
    """Test that the suffix is correctly extracted."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/path/to/distribution.tar.gz").suffix == ".gz"
    assert WorkspacePath(ws, "/no/suffix/here").suffix == ""


def test_suffixes() -> None:
    """Test that multiple suffixes are correctly extracted."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/path/to/distribution.tar.gz").suffixes == [".tar", ".gz"]
    assert WorkspacePath(ws, "/path/to/file.txt").suffixes == [".txt"]
    assert WorkspacePath(ws, "/no/suffix/here").suffixes == []


def test_stem() -> None:
    """Test that the stem is correctly extracted."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/path/to/distribution.tar.gz").stem == "distribution.tar"
    assert WorkspacePath(ws, "/path/to/file.txt").stem == "file"
    assert WorkspacePath(ws, "/no/suffix/here").stem == "here"


def test_with_name() -> None:
    """Test that the name in a path can be replaced."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/path/to/notebook.py").with_name("requirements.txt") == WorkspacePath(
        ws, "/path/to/requirements.txt"
    )
    assert WorkspacePath(ws, "relative/notebook.py").with_name("requirements.txt") == WorkspacePath(
        ws, "relative/requirements.txt"
    )


@pytest.mark.parametrize(
    ("path", "name"),
    [
        # Invalid names.
        ("/a/path", "invalid/replacement"),
        ("/a/path", ""),
        ("/a/path", "."),
        # Invalid paths for using with_name()
        ("/", "file.txt"),
        ("", "file.txt"),
    ],
)
def test_with_name_errors(path, name) -> None:
    """Test that various forms of invalid .with_name() invocations are detected."""
    ws = create_autospec(WorkspaceClient)

    with pytest.raises(ValueError):
        _ = WorkspacePath(ws, path).with_name(name)


def test_with_stem() -> None:
    """Test that the stem in a path can be replaced."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/dir/file.txt").with_stem("README") == WorkspacePath(ws, "/dir/README.txt")


def test_with_suffix() -> None:
    """Test that the suffix of a path can be replaced, and that some errors are handled."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/dir/README.txt").with_suffix(".md") == WorkspacePath(ws, "/dir/README.md")
    with pytest.raises(ValueError, match="[Ii]nvalid suffix"):
        _ = WorkspacePath(ws, "/dir/README.txt").with_suffix("txt")
    with pytest.raises(ValueError, match="empty name"):
        _ = WorkspacePath(ws, "/").with_suffix(".txt")


def test_as_uri() -> None:
    """Verify that the URI that corresponds to a path can be generated."""
    ws = create_autospec(WorkspaceClient)
    ws.config.host = "https://example.com/instance"

    ws_path = "/tmp/file with spaces.md"
    expected_url = "https://example.com/instance#workspace/tmp/file%20with%20spaces.md"

    assert WorkspacePath(ws, ws_path).as_uri() == expected_url


@pytest.mark.parametrize(
    ("path", "parent"),
    [
        ("/foo/bar/baz", "/foo/bar"),
        ("/", "/"),
        (".", "."),
        ("foo/bar", "foo"),
        ("foo", "."),
    ],
)
def test_parent(path, parent) -> None:
    """Test that the parent of a path is properly calculated."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, path).parent == WorkspacePath(ws, parent)


@pytest.mark.parametrize(
    ("path", "parents"),
    [
        ("/foo/bar/baz", ("/foo/bar", "/foo", "/")),
        ("/", ()),
        (".", ()),
        ("foo/bar", ("foo", ".")),
        ("foo", (".",)),
    ],
)
def test_parents(path, parents) -> None:
    """Test that each of the parents of a path is returned."""
    ws = create_autospec(WorkspaceClient)

    expected_parents = tuple(WorkspacePath(ws, parent) for parent in parents)
    assert tuple(WorkspacePath(ws, path).parents) == expected_parents


def test_is_relative_to() -> None:
    """Test detection of whether a path is relative to the target."""
    ws = create_autospec(WorkspaceClient)

    # Basics where it's true.
    assert WorkspacePath(ws, "/home/bob").is_relative_to("/")
    assert WorkspacePath(ws, "/home/bob").is_relative_to("/home")
    assert WorkspacePath(ws, "/home/bob").is_relative_to("/./home")
    assert WorkspacePath(ws, "foo/bar/baz").is_relative_to("foo")
    assert WorkspacePath(ws, "foo/bar/baz").is_relative_to("foo/bar")
    assert WorkspacePath(ws, "foo/bar/baz").is_relative_to("foo/./bar")

    # Some different situations where it isn't.
    assert not WorkspacePath(ws, "/home/bob").is_relative_to("home")  # Different anchor.
    assert not WorkspacePath(ws, "/home/bob").is_relative_to("/usr")  # Not a prefix.


def test_is_absolute() -> None:
    """Test detection of absolute versus relative paths."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/foo/bar").is_absolute()
    assert WorkspacePath(ws, "/").is_absolute()
    assert not WorkspacePath(ws, "foo/bar").is_absolute()
    assert not WorkspacePath(ws, ".").is_absolute()


def test_is_reserved() -> None:
    """Test detection of reserved paths (which don't exist with Workspace paths)."""
    ws = create_autospec(WorkspaceClient)

    assert not WorkspacePath(ws, "NUL").is_reserved()


def test_joinpath() -> None:
    """Test that paths can be joined."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/home").joinpath("bob") == WorkspacePath(ws, "/home/bob")
    assert WorkspacePath(ws, "/home").joinpath(WorkspacePath(ws, "bob")) == WorkspacePath(ws, "/home/bob")
    assert WorkspacePath(ws, "/home").joinpath(PurePosixPath("bob")) == WorkspacePath(ws, "/home/bob")
    assert WorkspacePath(ws, "/usr").joinpath("local", "bin") == WorkspacePath(ws, "/usr/local/bin")
    assert WorkspacePath(ws, "home").joinpath("jane") == WorkspacePath(ws, "home/jane")


def test_join_dsl() -> None:
    """Test that the /-based DSL can be used to build new paths."""
    ws = create_autospec(WorkspaceClient)

    # First the forward style options.
    assert WorkspacePath(ws, "/home/bob") / "data" == WorkspacePath(ws, "/home/bob/data")
    assert WorkspacePath(ws, "/home/bob") / "data" / "base" == WorkspacePath(ws, "/home/bob/data/base")
    assert WorkspacePath(ws, "/home/bob") / "data/base" == WorkspacePath(ws, "/home/bob/data/base")
    assert WorkspacePath(ws, "home") / "bob" == WorkspacePath(ws, "home/bob")
    # New root
    assert WorkspacePath(ws, "whatever") / "/home" == WorkspacePath(ws, "/home")
    # Mix types: eventual type is less-associative
    assert WorkspacePath(ws, "/home/bob") / PurePosixPath("data") == WorkspacePath(ws, "/home/bob/data")

    # Building from the other direction; same as above.
    assert "/home/bob" / WorkspacePath(ws, "data") == WorkspacePath(ws, "/home/bob/data")
    assert "/home/bob" / WorkspacePath(ws, "data") / "base" == WorkspacePath(ws, "/home/bob/data/base")
    assert "/home/bob" / WorkspacePath(ws, "data/base") == WorkspacePath(ws, "/home/bob/data/base")
    assert "home" / WorkspacePath(ws, "bob") == WorkspacePath(ws, "home/bob")
    # New root
    assert "whatever" / WorkspacePath(ws, "/home") == WorkspacePath(ws, "/home")
    # Mix types: eventual type is less-associative
    assert PurePosixPath("/home/bob") / WorkspacePath(ws, "data") == PurePosixPath("/home/bob/data")


def test_match() -> None:
    """Test that glob matching works."""
    ws = create_autospec(WorkspaceClient)

    # Relative patterns, match from the right.
    assert WorkspacePath(ws, "foo/bar/file.txt").match("*.txt")
    assert WorkspacePath(ws, "/foo/bar/file.txt").match("bar/*.txt")
    assert not WorkspacePath(ws, "/foo/bar/file.txt").match("foo/*.txt")

    # Absolute patterns, match from the left (and only against absolute paths)
    assert WorkspacePath(ws, "/file.txt").match("/*.txt")
    assert not WorkspacePath(ws, "foo/bar/file.txt").match("/*.txt")

    # Case-sensitive by default, but can be overridden.
    assert not WorkspacePath(ws, "file.txt").match("*.TXT")
    # assert WorkspacePath(ws, "file.txt").match("*.TXT", case_sensitive=False)

    # No recursive globs.
    assert not WorkspacePath(ws, "/foo/bar/file.txt").match("/**/*.txt")


def test_iterdir() -> None:
    """Test that iterating through a directory works."""
    ws = create_autospec(WorkspaceClient)
    ws.workspace.list.return_value = iter(
        (
            ObjectInfo(path="/home/bob"),
            ObjectInfo(path="/home/jane"),
            ObjectInfo(path="/home/ted"),
            ObjectInfo(path="/home/fred"),
        )
    )

    children = set(WorkspacePath(ws, "/home").iterdir())

    assert children == {
        WorkspacePath(ws, "/home/bob"),
        WorkspacePath(ws, "/home/jane"),
        WorkspacePath(ws, "/home/ted"),
        WorkspacePath(ws, "/home/fred"),
    }


def test_exists_when_path_exists() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(path="/test/path")
    assert workspace_path.exists()


def test_exists_caches_info() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(path="/test/path", object_type=ObjectType.FILE)
    _ = workspace_path.exists()

    ws.workspace.get_status.reset_mock()
    _ = workspace_path.is_file()
    assert not ws.workspace.get_status.called


def test_exists_when_path_does_not_exist() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.side_effect = NotFound("Simulated NotFound")
    assert not workspace_path.exists()


def test_mkdir_creates_directory() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.mkdir()
    ws.workspace.mkdirs.assert_called_once_with("/test/path")


def test_rmdir_removes_directory() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.rmdir()
    ws.workspace.delete.assert_called_once_with("/test/path", recursive=False)


def test_is_dir_when_path_is_directory() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.DIRECTORY)
    assert workspace_path.is_dir()


def test_is_dir_when_path_is_not_directory() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.FILE)
    assert not workspace_path.is_dir()


def test_is_file_when_path_is_file() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.FILE)
    assert workspace_path.is_file()


def test_is_file_when_path_is_not_file() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.DIRECTORY)
    assert not workspace_path.is_file()


def test_is_notebook_when_path_is_notebook() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.NOTEBOOK)
    assert workspace_path.is_notebook()


def test_is_notebook_when_path_is_not_notebook() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.FILE)
    assert not workspace_path.is_notebook()


def test_open_file_in_read_binary_mode() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b"test"
    workspace_path = WorkspacePath(ws, "/test/path")
    assert workspace_path.read_bytes() == b"test"


def test_open_file_in_write_binary_mode() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.write_bytes(b"test")
    ws.workspace.upload.assert_called_with("/test/path", b"test", format=ImportFormat.AUTO)


def test_open_file_in_read_text_mode() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b"test"
    assert workspace_path.read_text() == "test"


def test_open_file_in_write_text_mode() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.write_text("test")
    ws.workspace.upload.assert_called_with("/test/path", "test", format=ImportFormat.AUTO)


def test_open_file_in_invalid_mode() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    with pytest.raises(ValueError):
        workspace_path.open(mode="invalid")


def test_suffix_when_file_has_extension() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path.py")
    assert workspace_path.suffix == ".py"


def test_suffix_when_file_is_notebook_and_language_matches() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.return_value = ObjectInfo(language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)

    workspace_path = WorkspacePath(ws, "/test/path")
    assert workspace_path.suffix == ".py"


def test_suffix_when_file_is_notebook_and_language_does_not_match() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.return_value = ObjectInfo(language=None, object_type=ObjectType.NOTEBOOK)

    workspace_path = WorkspacePath(ws, "/test/path")
    assert workspace_path.suffix == ""


def test_suffix_when_file_is_not_notebook() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.return_value = ObjectInfo(language=Language.PYTHON, object_type=ObjectType.FILE)

    workspace_path = WorkspacePath(ws, "/test/path")
    assert workspace_path.suffix == ""


def test_mkdir_creates_directory_with_valid_mode() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.mkdir(mode=0o600)
    ws.workspace.mkdirs.assert_called_once_with("/test/path")


def test_mkdir_raises_error_with_invalid_mode() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    with pytest.raises(ValueError):
        workspace_path.mkdir(mode=0o700)


def test_rmdir_removes_directory_non_recursive() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.rmdir()
    ws.workspace.delete.assert_called_once_with("/test/path", recursive=False)


def test_rmdir_removes_directory_recursive() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.rmdir(recursive=True)
    ws.workspace.delete.assert_called_once_with("/test/path", recursive=True)


def test_rename_file() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b"test"
    assert workspace_path.rename("/new/path") == WorkspacePath(ws, "/new/path")
    ws.workspace.upload.assert_called_once_with("/new/path", b"test", format=ImportFormat.AUTO, overwrite=False)


def test_replace_file() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b"test"
    assert workspace_path.replace("/new/path") == WorkspacePath(ws, "/new/path")
    ws.workspace.upload.assert_called_once_with("/new/path", b"test", format=ImportFormat.AUTO, overwrite=True)


def test_unlink_existing_file() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.unlink()
    ws.workspace.delete.assert_called_once_with("/test/path")
    assert not ws.workspace.get_status.called


def test_unlink_non_existing_file() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.delete.side_effect = ResourceDoesNotExist("Simulated ResourceDoesNotExist")
    ws.workspace.get_status.side_effect = NotFound("Simulated NotFound")
    with pytest.raises(FileNotFoundError):
        workspace_path.unlink()


def test_relative_to() -> None:
    """Test that it is possible to get the relative path between two paths."""
    ws = create_autospec(WorkspaceClient)

    # Basics.
    assert WorkspacePath(ws, "/home/bob").relative_to("/") == WorkspacePath(ws, "home/bob")
    assert WorkspacePath(ws, "/home/bob").relative_to("/home") == WorkspacePath(ws, "bob")
    assert WorkspacePath(ws, "/home/bob").relative_to("/./home") == WorkspacePath(ws, "bob")
    assert WorkspacePath(ws, "foo/bar/baz").relative_to("foo") == WorkspacePath(ws, "bar/baz")
    assert WorkspacePath(ws, "foo/bar/baz").relative_to("foo/bar") == WorkspacePath(ws, "baz")
    assert WorkspacePath(ws, "foo/bar/baz").relative_to("foo/./bar") == WorkspacePath(ws, "baz")

    # Walk-up (3.12+) behaviour.
    assert WorkspacePath(ws, "/home/bob").relative_to("/usr", walk_up=True) == WorkspacePath(ws, "../home/bob")

    # Check some errors.
    with pytest.raises(ValueError, match="different anchors"):
        _ = WorkspacePath(ws, "/home/bob").relative_to("home")
    with pytest.raises(ValueError, match="not in the subpath"):
        _ = WorkspacePath(ws, "/home/bob").relative_to("/usr")
    with pytest.raises(ValueError, match="cannot be walked"):
        _ = WorkspacePath(ws, "/home/bob").relative_to("/home/../usr", walk_up=True)


def test_as_fuse_in_databricks_runtime() -> None:
    with patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "14.3"}):
        ws = create_autospec(WorkspaceClient)
        workspace_path = WorkspacePath(ws, "/test/path")
        result = workspace_path.as_fuse()
        assert str(result) == "/Workspace/test/path"


def test_as_fuse_outside_databricks_runtime() -> None:
    with patch.dict("os.environ", {}, clear=True):
        ws = create_autospec(WorkspaceClient)
        workspace_path = WorkspacePath(ws, "/test/path")
        result = workspace_path.as_fuse()
        assert str(result) == "/Workspace/test/path"


def test_home_directory() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me.return_value.user_name = "test_user"
    workspace_path = WorkspacePath(ws, "/test/path")
    result = workspace_path.home()
    assert str(result) == "/Users/test_user"


def test_resolve() -> None:
    """This is only supported for absolute paths.

    Otherwise it depends on the current working directory which isn't supported."""
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.side_effect = (
        ObjectInfo(path="/path/that/exists", object_type=ObjectType.FILE),
        NotFound("Simulated NotFound"),
    )

    assert WorkspacePath(ws, "/absolute/path").resolve() == WorkspacePath(ws, "/absolute/path")
    assert WorkspacePath(ws, "/path/that/exists").resolve(strict=True) == WorkspacePath(ws, "/path/that/exists")
    with pytest.raises(FileNotFoundError):
        _ = WorkspacePath(ws, "/path/that/does/not/exist").resolve(strict=True)
    with pytest.raises(NotImplementedError):
        _ = WorkspacePath(ws, "relative/path").resolve()


def test_absolute() -> None:
    """This is only supported for absolute paths.

    Otherwise it depends on the current working directory which isn't supported."""
    ws = create_autospec(WorkspaceClient)

    assert WorkspacePath(ws, "/absolute/path").absolute() == WorkspacePath(ws, "/absolute/path")
    with pytest.raises(NotImplementedError):
        _ = WorkspacePath(ws, "relative/path").absolute()


def test_is_dir_when_object_type_is_directory() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.DIRECTORY
    assert workspace_path.is_dir() is True


def test_is_dir_when_object_type_is_not_directory() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.FILE
    assert workspace_path.is_dir() is False


def test_is_dir_when_databricks_error_occurs() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.side_effect = NotFound("Simulated NotFound")
    assert workspace_path.is_dir() is False


def test_is_file_when_object_type_is_file() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.FILE
    assert workspace_path.is_file() is True


def test_is_file_when_object_type_is_not_file() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.DIRECTORY
    assert workspace_path.is_file() is False


def test_is_file_when_databricks_error_occurs() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.side_effect = NotFound("Simulated NotFound")
    assert workspace_path.is_file() is False


def test_is_notebook_when_object_type_is_notebook() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.NOTEBOOK
    assert workspace_path.is_notebook() is True


def test_is_notebook_when_object_type_is_not_notebook() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.FILE
    assert workspace_path.is_notebook() is False


def test_is_notebook_when_databricks_error_occurs() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.side_effect = NotFound("Simulated NotFound")
    assert workspace_path.is_notebook() is False


class StubWorkspaceFilesystem:
    """Stub the basic Workspace filesystem operations."""

    __slots__ = ("_paths",)

    def __init__(self, *known_paths: str | ObjectInfo) -> None:
        """Initialize a virtual filesystem with a set of known paths.

        Each known path can either be a string or a complete ObjectInfo instance; if a string then the path is
        treated as a directory if it has a trailing-/ or a file otherwise.
        """
        fs_entries = [self._normalize_path(p) for p in known_paths]
        keyed_entries = {o.path: o for o in fs_entries if o.path is not None}
        self._normalize_paths(keyed_entries)
        self._paths = keyed_entries

    @classmethod
    def _normalize_path(cls, path: str | ObjectInfo) -> ObjectInfo:
        if isinstance(path, ObjectInfo):
            return path
        return ObjectInfo(
            path=path.rstrip("/") if path != "/" else path,
            object_type=ObjectType.DIRECTORY if path.endswith("/") else ObjectType.FILE,
        )

    @classmethod
    def _normalize_paths(cls, paths: dict[str, ObjectInfo]) -> None:
        """Validate entries are absolute and that intermediate directories are both present and typed correctly."""
        for p in list(paths):
            for parent in PurePath(p).parents:
                path = str(parent)
                paths.setdefault(path, ObjectInfo(path=path, object_type=ObjectType.DIRECTORY))

    def _stub_get_status(self, path: str) -> ObjectInfo:
        object_info = self._paths.get(path)
        if object_info is None:
            msg = f"Simulated path not found: {path}"
            raise NotFound(msg)
        return object_info

    def _stub_list(
        self, path: str, *, notebooks_modified_after: int | None = None, recursive: bool | None = False, **kwargs
    ) -> Iterator[ObjectInfo]:
        path = path.rstrip("/")
        path_len = len(path)
        for candidate, object_info in self._paths.items():
            # Only direct children, and excluding the path itself.
            if (
                len(candidate) > (path_len + 1)
                and candidate[:path_len] == path
                and candidate[path_len] == "/"
                and "/" not in candidate[path_len + 1 :]
            ):
                yield object_info

    def mock(self) -> WorkspaceExt:
        m = create_autospec(WorkspaceExt)
        m.get_status.side_effect = self._stub_get_status
        m.list.side_effect = self._stub_list
        return m


def test_globbing_literals() -> None:
    """Verify that trivial (literal) globs for one or more path segments match (or doesn't)."""
    ws = create_autospec(WorkspaceClient)
    ws.workspace = StubWorkspaceFilesystem("/home/bob/bin/labs", "/etc/passwd").mock()

    assert set(WorkspacePath(ws, "/home").glob("jane")) == set()
    assert set(WorkspacePath(ws, "/home").glob("bob")) == {WorkspacePath(ws, "/home/bob")}
    assert set(WorkspacePath(ws, "/home").glob("bob/")) == {WorkspacePath(ws, "/home/bob")}
    assert set(WorkspacePath(ws, "/home").glob("bob/bin")) == {WorkspacePath(ws, "/home/bob/bin")}
    assert set(WorkspacePath(ws, "/home").glob("bob/bin/labs/")) == set()
    assert set(WorkspacePath(ws, "/etc").glob("passwd")) == {WorkspacePath(ws, "/etc/passwd")}


def test_globbing_empty_error() -> None:
    """Verify that an empty glob triggers an immediate error."""
    ws = create_autospec(WorkspaceClient)

    with pytest.raises(ValueError, match="must not be empty"):
        _ = set(WorkspacePath(ws, "/etc/passwd").glob(""))


def test_globbing_absolute_error() -> None:
    """Verify that absolute-path globs triggers an immediate error."""
    ws = create_autospec(WorkspaceClient)

    with pytest.raises(NotImplementedError, match="Non-relative patterns are unsupported"):
        _ = set(WorkspacePath(ws, "/").glob("/tmp/*"))


def test_globbing_patterns() -> None:
    """Verify that globbing with globs works as expected, including across multiple path segments."""
    ws = create_autospec(WorkspaceClient)
    ws.workspace = StubWorkspaceFilesystem(
        "/home/bob/bin/labs",
        "/home/bob/bin/databricks",
        "/home/bot/",
        "/home/bat/",
        "/etc/passwd",
    ).mock()

    assert set(WorkspacePath(ws, "/home").glob("*")) == {
        WorkspacePath(ws, "/home/bob"),
        WorkspacePath(ws, "/home/bot"),
        WorkspacePath(ws, "/home/bat"),
    }
    assert set(WorkspacePath(ws, "/home/bob/bin").glob("*")) == {
        WorkspacePath(ws, "/home/bob/bin/databricks"),
        WorkspacePath(ws, "/home/bob/bin/labs"),
    }
    assert set(WorkspacePath(ws, "/home/bob").glob("*/*")) == {
        WorkspacePath(ws, "/home/bob/bin/databricks"),
        WorkspacePath(ws, "/home/bob/bin/labs"),
    }
    assert set(WorkspacePath(ws, "/home/bob/bin").glob("*a*")) == {
        WorkspacePath(ws, "/home/bob/bin/databricks"),
        WorkspacePath(ws, "/home/bob/bin/labs"),
    }
    assert set(WorkspacePath(ws, "/home").glob("bo[bt]")) == {
        WorkspacePath(ws, "/home/bob"),
        WorkspacePath(ws, "/home/bot"),
    }
    assert set(WorkspacePath(ws, "/home").glob("b[!o]t")) == {WorkspacePath(ws, "/home/bat")}


def test_glob_trailing_slash() -> None:
    """Verify that globs with a trailing slash only match directories."""
    ws = create_autospec(WorkspaceClient)
    ws.workspace = StubWorkspaceFilesystem(
        "/home/bob/bin/labs",
        "/home/bob/bin/databricks",
        "/home/bob/.profile",
    ).mock()

    assert set(WorkspacePath(ws, "/home/bob").glob("*/")) == {WorkspacePath(ws, "/home/bob/bin")}
    assert set(WorkspacePath(ws, "/home").glob("bob/*/")) == {WorkspacePath(ws, "/home/bob/bin")}
    # Negative test.
    assert WorkspacePath(ws, "/home/bob/.profile") in set(WorkspacePath(ws, "/home").glob("bob/*"))


def test_glob_parent_path_traversal_error() -> None:
    """Globs are normally allowed to include /../ segments to traverse directories; these aren't supported though."""
    ws = create_autospec(WorkspaceClient)

    with pytest.raises(ValueError, match="Parent traversal is not supported"):
        _ = set(WorkspacePath(ws, "/usr").glob("sbin/../bin"))


def test_recursive_glob() -> None:
    """Verify that recursive globs work properly."""
    ws = create_autospec(WorkspaceClient)
    ws.workspace = StubWorkspaceFilesystem(
        "/home/bob/bin/labs",
        "/usr/local/bin/labs",
        "/usr/local/sbin/labs",
    ).mock()

    assert set(WorkspacePath(ws, "/").glob("**/bin/labs")) == {
        WorkspacePath(ws, "/home/bob/bin/labs"),
        WorkspacePath(ws, "/usr/local/bin/labs"),
    }
    assert set(WorkspacePath(ws, "/").glob("usr/**/labs")) == {
        WorkspacePath(ws, "/usr/local/bin/labs"),
        WorkspacePath(ws, "/usr/local/sbin/labs"),
    }
    assert set(WorkspacePath(ws, "/").glob("usr/**")) == {
        WorkspacePath(ws, "/usr"),
        WorkspacePath(ws, "/usr/local"),
        WorkspacePath(ws, "/usr/local/bin"),
        WorkspacePath(ws, "/usr/local/sbin"),
    }


def test_double_recursive_glob() -> None:
    """Verify that double-recursive globs work as expected without duplicate results."""
    ws = create_autospec(WorkspaceClient)
    ws.workspace = StubWorkspaceFilesystem(
        "/some/long/path/with/repeated/path/segments/present",
    ).mock()

    assert tuple(WorkspacePath(ws, "/").glob("**/path/**/present")) == (
        WorkspacePath(ws, "/some/long/path/with/repeated/path/segments/present"),
    )


def test_glob_case_insensitive() -> None:
    """As of python 3.12, globbing is allowed to be case-insensitive irrespective of the underlying filesystem. Check this."""
    ws = create_autospec(WorkspaceClient)
    ws.workspace = StubWorkspaceFilesystem(
        "/home/bob/bin/labs",
        "/home/bob/bin/databricks",
        "/home/bot/",
        "/home/bat/",
        "/etc/passwd",
    ).mock()

    assert set(WorkspacePath(ws, "/home").glob("B*t", case_sensitive=False)) == {
        WorkspacePath(ws, "/home/bot"),
        WorkspacePath(ws, "/home/bat"),
    }
    assert set(WorkspacePath(ws, "/home").glob("bO[TB]", case_sensitive=False)) == {
        WorkspacePath(ws, "/home/bot"),
        WorkspacePath(ws, "/home/bob"),
    }
    assert set(WorkspacePath(ws, "/etc").glob("PasSWd", case_sensitive=False)) == {WorkspacePath(ws, "/etc/passwd")}


def test_globbing_when_nested_json_files_exist() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.DIRECTORY)
    ws.workspace.list.side_effect = [
        [
            ObjectInfo(path="/test/path/dir1", object_type=ObjectType.DIRECTORY),
            ObjectInfo(path="/test/path/dir2", object_type=ObjectType.DIRECTORY),
        ],
        [
            ObjectInfo(path="/test/path/dir1/file1.json", object_type=ObjectType.FILE),
        ],
        [
            ObjectInfo(path="/test/path/dir2/file2.json", object_type=ObjectType.FILE),
        ],
    ]
    result = [str(p) for p in workspace_path.glob("*/*.json")]
    assert result == ["/test/path/dir1/file1.json", "/test/path/dir2/file2.json"]


def test_rglob() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace = StubWorkspaceFilesystem(
        "/test/path/dir1/file1.json",
        "/test/path/dir2/file2.json",
    ).mock()

    assert set(WorkspacePath(ws, "/test").rglob("*.json")) == {
        WorkspacePath(ws, "/test/path/dir1/file1.json"),
        WorkspacePath(ws, "/test/path/dir2/file2.json"),
    }


def test_workspace_path_stat_has_fields():
    info = ObjectInfo(created_at=1234, modified_at=2345, size=3456)
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.return_value = info
    workspace_path = WorkspacePath(ws, "/test/path")
    stats = workspace_path.stat()
    assert stats.st_ctime == info.created_at
    assert stats.st_mtime == info.modified_at
    assert stats.st_size == info.size


def test_dbfs_path_stat_has_fields():
    info = FileInfo(modification_time=2345, file_size=3456)
    ws = create_autospec(WorkspaceClient)
    ws.dbfs.get_status.return_value = info
    dbfs_path = DBFSPath(ws, "/test/path")
    stats = dbfs_path.stat()
    assert stats.st_mtime == info.modification_time
    assert stats.st_size == info.file_size
