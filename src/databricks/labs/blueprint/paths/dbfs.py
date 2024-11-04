from __future__ import annotations

import builtins
import io
import logging
import os
import shutil
import stat
from collections.abc import Generator
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.files import FileInfo

from databricks.labs.blueprint.paths._base import DatabricksPath, P

logger = logging.getLogger(__name__)


class DBFSPath(DatabricksPath):
    """Experimental implementation of pathlib.Path for DBFS paths."""

    __slots__ = (
        # The cached _file_info value for the instance.
        "_cached_file_info",
    )
    _cached_file_info: FileInfo

    @classmethod
    def _from_file_info(cls, ws: WorkspaceClient, file_info: FileInfo) -> DBFSPath:
        """Special (internal-only) constructor that creates an instance based on FileInfo."""
        if not file_info.path:
            msg = f"Cannot initialise without file path: {file_info}"
            raise ValueError(msg)
        path = cls(ws, file_info.path)
        path._cached_file_info = file_info
        return path

    def as_fuse(self) -> Path:
        """Return FUSE-mounted path in Databricks Runtime."""
        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            logger.warning("This method is only available in Databricks Runtime")
        return Path("/dbfs", self.as_posix().lstrip("/"))

    def exists(self, *, follow_symlinks: bool = True) -> bool:
        """Return True if the path points to an existing file, directory, or notebook"""
        if not follow_symlinks:
            raise NotImplementedError("follow_symlinks=False is not supported for DBFS")
        try:
            self._cached_file_info = self._ws.dbfs.get_status(self.as_posix())
            return True
        except DatabricksError:
            return False

    def _mkdir(self) -> None:
        self._ws.dbfs.mkdirs(self.as_posix())

    def rmdir(self, recursive: bool = False) -> None:
        """Remove a DBFS directory"""
        self._ws.dbfs.delete(self.as_posix(), recursive=recursive)

    def rename(self: P, target: str | bytes | os.PathLike) -> P:
        """Rename this path as the target, unless the target already exists."""
        dst = self.with_segments(target)
        self._ws.dbfs.move(self.as_posix(), dst.as_posix())
        return dst

    def replace(self: P, target: str | bytes | os.PathLike) -> P:
        """Rename this path, overwriting the target if it exists and can be overwritten."""
        dst = self.with_segments(target)
        if self.is_dir():
            msg = f"DBFS directories cannot currently be replaced: {self} -> {dst}"
            raise ValueError(msg)
        # Can't use self._ws.dbfs.move_(): it doesn't honour the overwrite flag properly.
        with dst.open(mode="wb") as writer, self.open(mode="rb") as reader:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
        self.unlink()
        return dst

    def unlink(self, missing_ok: bool = False) -> None:
        """Remove a file in Databricks Workspace."""
        # Although this introduces a race-condition, we have to handle missing_ok in advance because the DBFS client
        # doesn't report any error if deleting a target that doesn't exist.
        if not missing_ok and not self.exists():
            raise FileNotFoundError(f"{self.as_posix()} does not exist")
        self._ws.dbfs.delete(self.as_posix())

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ):
        """Open a DBFS file.

        Only text and binary I/O are supported in basic read or write mode, along with 'x' to avoid overwriting."""
        is_write = "w" in mode
        is_read = "r" in mode or not is_write
        if is_read and is_write:
            msg = f"Unsupported mode: {mode} (simultaneous read and write)"
            raise ValueError(msg)
        is_binary = "b" in mode
        is_text = "t" in mode or not is_binary
        if is_binary and is_text:
            msg = f"Unsupported mode: {mode} (binary and text I/O)"
            raise ValueError(msg)
        is_overwrite = is_write and "x" not in mode
        binary_io = self._ws.dbfs.open(self.as_posix(), read=is_read, write=is_write, overwrite=is_overwrite)
        if is_text:
            return io.TextIOWrapper(binary_io, encoding=encoding, errors=errors, newline=newline)
        return binary_io

    def write_bytes(self, data):
        """Write the (binary) data to this path."""
        # The DBFS BinaryIO implementation only accepts bytes and rejects the (other) byte-like builtins.
        match data:
            case builtins.bytes | builtins.bytearray:
                binary_data = bytes(data)
            case _:
                binary_data = bytes(memoryview(data))
        with self.open("wb") as f:
            return f.write(binary_data)

    @property
    def _file_info(self) -> FileInfo:
        # this method is cached because it is used in multiple is_* methods.
        # DO NOT use this method in methods, where fresh result is required.
        try:
            return self._cached_file_info
        except AttributeError:
            self._cached_file_info = self._ws.dbfs.get_status(self.as_posix())
            return self._cached_file_info

    def stat(self, *, follow_symlinks=True) -> os.stat_result:
        seq: list[float] = [-1.0] * 10
        seq[stat.ST_SIZE] = self._file_info.file_size or -1  # 6
        seq[stat.ST_MTIME] = (
            float(self._file_info.modification_time) / 1000.0 if self._file_info.modification_time else -1.0
        )  # 8
        return os.stat_result(seq)

    def is_dir(self) -> bool:
        """Return True if the path points to a DBFS directory."""
        try:
            return bool(self._file_info.is_dir)
        except DatabricksError:
            return False

    def is_file(self) -> bool:
        """Return True if the path points to a file in Databricks Workspace."""
        return not self.is_dir()

    def iterdir(self) -> Generator[DBFSPath, None, None]:
        for child in self._ws.dbfs.list(self.as_posix()):
            yield self._from_file_info(self._ws, child)
