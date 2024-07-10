import os
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from unittest.mock import create_autospec, patch

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import (
    ImportFormat,
    Language,
    ObjectInfo,
    ObjectType,
)

from databricks.labs.blueprint.paths import WorkspacePath


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
def test_comparison(increasing_paths: tuple[str | list[str], str | list[str], str | list[str]]) -> None:
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

    with pytest.raises(NotImplementedError, match="Workspace paths are not path-like"):
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


def test_exists_when_path_exists():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = True
    assert workspace_path.exists()


def test_exists_when_path_does_not_exist():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.side_effect = NotFound("Simulated NotFound")
    assert not workspace_path.exists()


def test_mkdir_creates_directory():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.mkdir()
    ws.workspace.mkdirs.assert_called_once_with("/test/path")


def test_rmdir_removes_directory():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.rmdir()
    ws.workspace.delete.assert_called_once_with("/test/path", recursive=False)


def test_is_dir_when_path_is_directory():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.DIRECTORY)
    assert workspace_path.is_dir()


def test_is_dir_when_path_is_not_directory():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.FILE)
    assert not workspace_path.is_dir()


def test_is_file_when_path_is_file():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.FILE)
    assert workspace_path.is_file()


def test_is_file_when_path_is_not_file():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.DIRECTORY)
    assert not workspace_path.is_file()


def test_is_notebook_when_path_is_notebook():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.NOTEBOOK)
    assert workspace_path.is_notebook()


def test_is_notebook_when_path_is_not_notebook():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.FILE)
    assert not workspace_path.is_notebook()


def test_open_file_in_read_binary_mode():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b"test"
    workspace_path = WorkspacePath(ws, "/test/path")
    assert workspace_path.read_bytes() == b"test"


def test_open_file_in_write_binary_mode():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.write_bytes(b"test")
    ws.workspace.upload.assert_called_with("/test/path", b"test", format=ImportFormat.AUTO)


def test_open_file_in_read_text_mode():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b"test"
    assert workspace_path.read_text() == "test"


def test_open_file_in_write_text_mode():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.write_text("test")
    ws.workspace.upload.assert_called_with("/test/path", "test", format=ImportFormat.AUTO)


def test_open_file_in_invalid_mode():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    with pytest.raises(ValueError):
        workspace_path.open(mode="invalid")


def test_suffix_when_file_has_extension():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path.py")
    assert workspace_path.suffix == ".py"


def test_suffix_when_file_is_notebook_and_language_matches():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._cached_object_info = ObjectInfo(language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    assert workspace_path.suffix == ".py"


def test_suffix_when_file_is_notebook_and_language_does_not_match():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.language = None
    assert workspace_path.suffix == ""


def test_suffix_when_file_is_not_notebook():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    with patch("databricks.labs.blueprint.paths.WorkspacePath.is_notebook") as mock_is_notebook:
        mock_is_notebook.return_value = False
        assert workspace_path.suffix == ""


def test_mkdir_creates_directory_with_valid_mode():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.mkdir(mode=0o600)
    ws.workspace.mkdirs.assert_called_once_with("/test/path")


def test_mkdir_raises_error_with_invalid_mode():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    with pytest.raises(ValueError):
        workspace_path.mkdir(mode=0o700)


def test_rmdir_removes_directory_non_recursive():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.rmdir()
    ws.workspace.delete.assert_called_once_with("/test/path", recursive=False)


def test_rmdir_removes_directory_recursive():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path.rmdir(recursive=True)
    ws.workspace.delete.assert_called_once_with("/test/path", recursive=True)


def test_rename_file_without_overwrite():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b"test"
    workspace_path.rename("/new/path")
    ws.workspace.upload.assert_called_once_with("/new/path", b"test", format=ImportFormat.AUTO, overwrite=False)


def test_rename_file_with_overwrite():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b"test"
    workspace_path.rename("/new/path", overwrite=True)
    ws.workspace.upload.assert_called_once_with("/new/path", b"test", format=ImportFormat.AUTO, overwrite=True)


def test_unlink_existing_file():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = True
    workspace_path.unlink()
    ws.workspace.delete.assert_called_once_with("/test/path")


def test_unlink_non_existing_file():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
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


def test_as_fuse_in_databricks_runtime():
    with patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "14.3"}):
        ws = create_autospec(WorkspaceClient)
        workspace_path = WorkspacePath(ws, "/test/path")
        result = workspace_path.as_fuse()
        assert str(result) == "/Workspace/test/path"


def test_as_fuse_outside_databricks_runtime():
    with patch.dict("os.environ", {}, clear=True):
        ws = create_autospec(WorkspaceClient)
        workspace_path = WorkspacePath(ws, "/test/path")
        result = workspace_path.as_fuse()
        assert str(result) == "/Workspace/test/path"


def test_home_directory():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me.return_value.user_name = "test_user"
    workspace_path = WorkspacePath(ws, "/test/path")
    result = workspace_path.home()
    assert str(result) == "/Users/test_user"


def test_is_dir_when_object_type_is_directory():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.DIRECTORY
    assert workspace_path.is_dir() is True


def test_is_dir_when_object_type_is_not_directory():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.FILE
    assert workspace_path.is_dir() is False


def test_is_dir_when_databricks_error_occurs():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.side_effect = NotFound("Simulated NotFound")
    assert workspace_path.is_dir() is False


def test_is_file_when_object_type_is_file():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.FILE
    assert workspace_path.is_file() is True


def test_is_file_when_object_type_is_not_file():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.DIRECTORY
    assert workspace_path.is_file() is False


def test_is_file_when_databricks_error_occurs():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.side_effect = NotFound("Simulated NotFound")
    assert workspace_path.is_file() is False


def test_is_notebook_when_object_type_is_notebook():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.NOTEBOOK
    assert workspace_path.is_notebook() is True


def test_is_notebook_when_object_type_is_not_notebook():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.object_type = ObjectType.FILE
    assert workspace_path.is_notebook() is False


def test_is_notebook_when_databricks_error_occurs():
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.side_effect = NotFound("Simulated NotFound")
    assert workspace_path.is_notebook() is False


@pytest.mark.xfail(reason="Implementation pending.")
def test_globbing_when_nested_json_files_exist():
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
