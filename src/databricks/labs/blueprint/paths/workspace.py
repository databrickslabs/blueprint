from __future__ import annotations

import codecs
import locale
import logging
import os
import stat
from collections.abc import Generator
from io import StringIO
from pathlib import Path
from urllib.parse import quote_from_bytes as urlquote_from_bytes

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, ResourceDoesNotExist
from databricks.sdk.service.workspace import (
    ExportFormat,
    ImportFormat,
    Language,
    ObjectInfo,
    ObjectType,
)

from databricks.labs.blueprint.paths._base import (
    BinaryUploadIO,
    DatabricksPath,
    P,
    TextUploadIO,
)

logger = logging.getLogger(__name__)


class WorkspacePath(DatabricksPath):
    """Experimental implementation of pathlib.Path for Databricks Workspace."""

    __slots__ = (
        # The cached _object_info value for the instance.
        "_cached_object_info",
    )
    _cached_object_info: ObjectInfo

    _SUFFIXES = {".py": Language.PYTHON, ".sql": Language.SQL, ".scala": Language.SCALA, ".R": Language.R}

    @classmethod
    def _from_object_info(cls, ws: WorkspaceClient, object_info: ObjectInfo) -> WorkspacePath:
        """Special (internal-only) constructor that creates an instance based on ObjectInfo."""
        if not object_info.path:
            msg = f"Cannot initialise without object path: {object_info}"
            raise ValueError(msg)
        path = cls(ws, object_info.path)
        path._cached_object_info = object_info
        return path

    def as_uri(self) -> str:
        return f"{self._ws.config.host}#workspace{urlquote_from_bytes(bytes(self))}"

    def as_fuse(self) -> Path:
        """Return FUSE-mounted path in Databricks Runtime."""
        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            logger.warning("This method is only available in Databricks Runtime")
        return Path("/Workspace", self.as_posix().lstrip("/"))

    def exists(self, *, follow_symlinks: bool = True) -> bool:
        """Return True if the path points to an existing file, directory, or notebook"""
        if not follow_symlinks:
            raise NotImplementedError("follow_symlinks=False is not supported for Databricks Workspace")
        try:
            self._cached_object_info = self._ws.workspace.get_status(self.as_posix())
            return True
        except DatabricksError:
            return False

    def _mkdir(self) -> None:
        self._ws.workspace.mkdirs(self.as_posix())

    def rmdir(self, recursive: bool = False) -> None:
        """Remove a directory in Databricks Workspace"""
        self._ws.workspace.delete(self.as_posix(), recursive=recursive)

    def _rename(self: P, target: str | bytes | os.PathLike, overwrite: bool) -> P:
        """Rename a file in Databricks Workspace"""
        dst = self.with_segments(target)
        if self.is_dir():
            msg = f"Workspace directories cannot currently be renamed: {self} -> {dst}"
            raise ValueError(msg)
        with self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO) as f:
            self._ws.workspace.upload(dst.as_posix(), f.read(), format=ImportFormat.AUTO, overwrite=overwrite)
        self.unlink()
        return dst

    def rename(self, target: str | bytes | os.PathLike):
        """Rename this path as the target, unless the target already exists."""
        return self._rename(target, overwrite=False)

    def replace(self, target: str | bytes | os.PathLike):
        """Rename this path, overwriting the target if it exists and can be overwritten."""
        return self._rename(target, overwrite=True)

    def unlink(self, missing_ok: bool = False) -> None:
        """Remove a file in Databricks Workspace."""
        try:
            self._ws.workspace.delete(self.as_posix())
        except ResourceDoesNotExist as e:
            if not missing_ok:
                raise FileNotFoundError(f"{self.as_posix()} does not exist") from e

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ):
        """Open a file in Databricks Workspace. Only text and binary modes are supported."""
        if "b" in mode and "r" in mode:
            return self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO)
        if "b" in mode and "w" in mode:
            return BinaryUploadIO(self._ws, self.as_posix())
        if "r" in mode:
            with self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO) as f:
                data = f.read()
                if encoding is None:
                    if data.startswith(codecs.BOM_UTF32_LE) or data.startswith(codecs.BOM_UTF32_BE):
                        encoding = "utf-32"
                    elif data.startswith(codecs.BOM_UTF16_LE) or data.startswith(codecs.BOM_UTF16_BE):
                        encoding = "utf-16"
                    elif data.startswith(codecs.BOM_UTF8):
                        encoding = "utf-8-sig"
                if encoding is None or encoding == "locale":
                    encoding = locale.getpreferredencoding(False)
                return StringIO(data.decode(encoding))
        if "w" in mode:
            return TextUploadIO(self._ws, self.as_posix())
        raise ValueError(f"invalid mode: {mode}")

    def read_text(self, encoding=None, errors=None):
        with self.open(mode="r", encoding=encoding, errors=errors) as f:
            return f.read()

    @property
    def suffix(self) -> str:
        """Return the file extension. If the file is a notebook, return the suffix based on the language."""
        suffix = super().suffix
        if suffix:
            return suffix
        if not self.is_notebook():
            return ""
        for sfx, lang in self._SUFFIXES.items():
            try:
                if self._object_info.language == lang:
                    return sfx
            except DatabricksError:
                return ""
        return ""

    @property
    def _object_info(self) -> ObjectInfo:
        # this method is cached because it is used in multiple is_* methods.
        # DO NOT use this method in methods, where fresh result is required.
        try:
            return self._cached_object_info
        except AttributeError:
            self._cached_object_info = self._ws.workspace.get_status(self.as_posix())
            return self._object_info

    def stat(self, *, follow_symlinks=True) -> os.stat_result:
        seq: list[float] = [-1.0] * 10
        seq[stat.ST_SIZE] = self._object_info.size or -1  # 6
        seq[stat.ST_MTIME] = (
            float(self._object_info.modified_at) / 1000.0 if self._object_info.modified_at else -1.0
        )  # 8
        seq[stat.ST_CTIME] = float(self._object_info.created_at) / 1000.0 if self._object_info.created_at else -1.0  # 9
        return os.stat_result(seq)

    def is_dir(self) -> bool:
        """Return True if the path points to a directory in Databricks Workspace."""
        try:
            return self._object_info.object_type == ObjectType.DIRECTORY
        except DatabricksError:
            return False

    def is_file(self) -> bool:
        """Return True if the path points to a file in Databricks Workspace."""
        try:
            return self._object_info.object_type == ObjectType.FILE
        except DatabricksError:
            return False

    def is_notebook(self) -> bool:
        """Return True if the path points to a notebook in Databricks Workspace."""
        try:
            return self._object_info.object_type == ObjectType.NOTEBOOK
        except DatabricksError:
            return False

    def iterdir(self) -> Generator[WorkspacePath, None, None]:
        for child in self._ws.workspace.list(self.as_posix()):
            yield self._from_object_info(self._ws, child)
