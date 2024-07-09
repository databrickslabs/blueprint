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


def test_exists_when_path_exists() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = True
    assert workspace_path.exists()


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
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info = ObjectInfo(language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    assert workspace_path.suffix == ".py"


def test_suffix_when_file_is_notebook_and_language_does_not_match() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    workspace_path._object_info.language = None
    assert workspace_path.suffix == ""


def test_suffix_when_file_is_not_notebook() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    with patch("databricks.labs.blueprint.paths.WorkspacePath.is_notebook") as mock_is_notebook:
        mock_is_notebook.return_value = False
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


def test_rename_file_without_overwrite() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b"test"
    workspace_path.rename("/new/path")
    ws.workspace.upload.assert_called_once_with("/new/path", b"test", format=ImportFormat.AUTO, overwrite=False)


def test_rename_file_with_overwrite() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b"test"
    workspace_path.rename("/new/path", overwrite=True)
    ws.workspace.upload.assert_called_once_with("/new/path", b"test", format=ImportFormat.AUTO, overwrite=True)


def test_unlink_existing_file() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.return_value = True
    workspace_path.unlink()
    ws.workspace.delete.assert_called_once_with("/test/path")


def test_unlink_non_existing_file() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path")
    ws.workspace.get_status.side_effect = NotFound("Simulated NotFound")
    with pytest.raises(FileNotFoundError):
        workspace_path.unlink()


def test_relative_to() -> None:
    ws = create_autospec(WorkspaceClient)
    workspace_path = WorkspacePath(ws, "/test/path/subpath")
    result = workspace_path.relative_to("/test/path")
    assert str(result) == "subpath"


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
