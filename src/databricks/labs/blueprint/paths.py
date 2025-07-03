from __future__ import annotations

import abc
import builtins
import codecs
import fnmatch
import io
import logging
import os
import re
import shutil
import stat
from abc import abstractmethod
from collections.abc import Generator, Iterable, Sequence
from io import BytesIO, StringIO
from pathlib import Path, PurePath
from typing import BinaryIO, Literal, NoReturn, TextIO, TypeVar
from urllib.parse import quote_from_bytes as urlquote_from_bytes

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, ResourceDoesNotExist
from databricks.sdk.service.files import FileInfo
from databricks.sdk.service.workspace import (
    ExportFormat,
    ImportFormat,
    Language,
    ObjectInfo,
    ObjectType,
)

from databricks.labs.blueprint import _posixpath

logger = logging.getLogger(__name__)


def _na(fn: str):
    def _inner(*_, **__):
        __tracebackhide__ = True  # pylint: disable=unused-variable
        raise NotImplementedError(f"{fn}() is not available for Databricks Workspace")

    return _inner


class _UploadIO(abc.ABC):
    def __init__(self, ws: WorkspaceClient, path: str):
        self._ws = ws
        self._path = path

    def close(self):
        # pylint: disable-next=no-member
        io_stream = self.getvalue()  # noqa
        self._ws.workspace.upload(self._path, io_stream, format=ImportFormat.AUTO)

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self._path} on {self._ws}>"


class _BinaryUploadIO(_UploadIO, BytesIO):  # type: ignore
    def __init__(self, ws: WorkspaceClient, path: str):
        _UploadIO.__init__(self, ws, path)
        BytesIO.__init__(self)


class _TextUploadIO(_UploadIO, StringIO):  # type: ignore
    def __init__(self, ws: WorkspaceClient, path: str):
        _UploadIO.__init__(self, ws, path)
        StringIO.__init__(self)


P = TypeVar("P", bound="_DatabricksPath")


class _DatabricksPath(Path, abc.ABC):  # pylint: disable=too-many-public-methods
    """Base-class for experimental pathlib.Path implementations covering Databricks Workspace paths and DBFS."""

    # Implementation notes:
    #  - The builtin Path classes are not designed for extension, which in turn makes everything a little cumbersome.
    #  - The internals of the builtin pathlib have changed dramatically across supported Python versions (3.10-3.12 at
    #    the time of writing) so relying on those details is brittle. (Python 3.13 also includes a significant
    #    refactoring.)
    #  - Until 3.11 the implementation was decomposed and delegated to two internal interfaces:
    #     1. Flavour (scope=class) which encapsulates the path style and manipulation.
    #     2. Accessor (scope=instance) to which I/O-related calls are delegated.
    #    These interfaces are internal/protected.
    #  - Since 3.12 the implementation of these interfaces have been removed:
    #     1. Flavour has been replaced with posixpath and ntpath (normally imported as os.path). Still class-scoped.
    #     2. Accessor has been replaced with inline implementations based directly on the 'os' module.
    #  - Since 3.13 comparisons and equality include a check on the identity of the 'parser' property instead of using
    #    the flavour.
    #  - Comparisons for builtin paths use different strategies, depending on the python version.
    #     - Python 3.10, 3.11: Loose ("PurePath") type and flavour equality check, _cparts property check.
    #     - Python 3.12: Loose ("PurePath") type and flavour equality check, _str_normcase property check.
    #     - Python 3.13: Loose ("PurePath") type and parser identity check, _str_normcase property check.
    #    Although we can override comparisons when we are on the LHS, when a builtin path is on the LHS its comparison
    #    is first attempted. We deal with this with a combination of techniques:
    #     - Stubbing/emulating the internal properties that they use. (This means they don't trigger exceptions.)
    #     - Trying to force the builtin implementation to return NotImplemented: when this happens, Python will attempt
    #       the reversed comparison (by swapping LHS/RHS) and therefore our implementation is invoked.
    #     - Ensuring the parser property doesn't have the same identity as builtin parsers. (From python 3.13 only
    #       paths with the same parser object are comparable.)
    #
    # This implementation for Databricks-style paths does the following:
    #     1. Flavour is basically posix-style, with the caveat that we don't bother with the special //-prefix handling.
    #     2. The Accessor is replaced by delegation to existing routines available via the workspace client.
    #     3. Python 3.12 introduces some new API elements. Because these are source-compatible with earlier versions
    #        these are forward-ported and implemented.
    #
    # The current class hierarchy implements behaviour (and differentiation) by inheritance rather than composition.
    #
    __slots__ = (  # pylint: disable=redefined-slots-in-subclass
        # For us this is always the empty string. Consistent with the superclass attribute for Python 3.10-3.13b.
        "_drv",
        # The (normalized) root property for the path. Consistent with the superclass attribute for Python 3.10-3.13b.
        "_root",
        # The (normalized) path components (relative to the root) for the path.
        #  - For python <=3.11 this supersedes _parts
        #  - For python 3.12+ this supersedes _raw_paths
        "_path_parts",
        # The cached str() value of the instance. Consistent with the superclass attribute for Python 3.10-3.13b.
        "_str",
        # The cached hash() value for the instance. Consistent with the superclass attribute for Python 3.10-3.13b.
        "_hash",
        # The workspace client that we use to perform I/O operations on the path.
        "_ws",
    )
    _str: str
    _hash: int

    parser = _posixpath

    # Compatibility attribute, for when superclass implementations get invoked on python <= 3.11.
    _flavour = object()

    # Public APIs that we don't support.
    as_uri = _na("as_uri")
    cwd = _na("cwd")
    chmod = _na("chmod")
    lchmod = _na("lchmod")
    lstat = _na("lstat")
    owner = _na("owner")
    group = _na("group")
    readlink = _na("readlink")
    symlink_to = _na("symlink_to")
    hardlink_to = _na("hardlink_to")
    touch = _na("touch")
    link_to = _na("link_to")
    samefile = _na("samefile")

    def __new__(cls, *args, **kwargs):
        # Force all initialisation to go via __init__() irrespective of the (Python-specific) base version.
        return object.__new__(cls)

    # pylint: disable=super-init-not-called
    def __init__(self, ws: WorkspaceClient, *args: str | bytes | os.PathLike) -> None:
        # We deliberately do _not_ call the super initializer because we're taking over complete responsibility for the
        # implementation of the public API.

        # Convert the arguments into string-based path segments, irrespective of their type.
        raw_paths = self._to_raw_paths(*args)

        # Normalise the paths that we have.
        root, path_parts = self._parse_and_normalize(raw_paths)
        self._drv = ""
        self._root = root
        self._path_parts = path_parts
        self._ws = ws

    @staticmethod
    def _to_raw_paths(*args: str | bytes | os.PathLike) -> list[str]:
        raw_paths: list[str] = []
        for arg in args:
            if isinstance(arg, PurePath):
                raw_paths.extend(arg.parts)
            else:
                try:
                    path = os.fspath(arg)
                except TypeError:
                    path = arg
                if not isinstance(path, str):
                    msg = (
                        f"argument should be a str or an os.PathLib object where __fspath__ returns a str, "
                        f"not {type(path).__name__!r}"
                    )
                    raise TypeError(msg)
                raw_paths.append(path)
        return raw_paths

    @classmethod
    def _parse_and_normalize(cls, parts: list[str]) -> tuple[str, tuple[str, ...]]:
        """Parse and normalize a list of path components.

        Args:
            parts: a list of path components to parse and normalize.
        Returns:
            A tuple containing:
              - The normalized drive (always '')
              - The normalized root for this path, or '' if there isn't any.
              - The normalized path components, if any, (relative) to the root.
        """
        match parts:
            case []:
                path = ""
            case [part]:
                path = part
            case [*parts]:
                path = cls.parser.join(*parts)
        if not path:
            return "", ()
        root, rel = cls._splitroot(path, sep=cls.parser.sep)
        # No need to split drv because we don't support it.
        parsed = tuple(str(x) for x in rel.split(cls.parser.sep) if x and x != ".")
        return root, parsed

    @classmethod
    def _splitroot(cls, part: str, sep: str) -> tuple[str, str]:
        # Based on the upstream implementation, with the '//'-specific bit elided because we don't need to
        # bother with Posix semantics.
        if part and part[0] == sep:
            return sep, part.lstrip(sep)
        return "", part

    @abstractmethod
    def as_fuse(self) -> Path: ...

    @abstractmethod
    def exists(self, *, follow_symlinks: bool = True) -> bool: ...

    @abstractmethod
    def _mkdir(self) -> None: ...

    @abstractmethod
    def rmdir(self, recursive: bool = False) -> None: ...

    @abstractmethod
    def unlink(self, missing_ok: bool = False) -> None: ...

    @abstractmethod
    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ): ...

    @abstractmethod
    def is_dir(self) -> bool: ...

    @abstractmethod
    def is_file(self) -> bool: ...

    @abstractmethod
    def rename(self: P, target: str | bytes | os.PathLike) -> P: ...

    @abstractmethod
    def replace(self: P, target: str | bytes | os.PathLike) -> P: ...

    @abstractmethod
    def iterdir(self: P) -> Generator[P, None, None]: ...

    def __reduce__(self) -> NoReturn:
        # Cannot support pickling because we can't pickle the workspace client.
        msg = f"Pickling {self.__class__.__qualname__} paths is not supported."
        raise NotImplementedError(msg)

    def __fspath__(self):
        # Cannot support this: Workspace objects aren't accessible via the filesystem.
        #
        # This method is part of the os.PathLike protocol. Functions which accept a PathLike argument use os.fsname()
        # to convert (via this method) the object into a file system path that can be used with the low-level os.*
        # methods.
        #
        # Relevant online documentation:
        #  - PEP 519 (https://peps.python.org/pep-0519/)
        #  - os.fspath (https://docs.python.org/3/library/os.html#os.fspath)
        # TODO: Allow this to work when within an appropriate Databricks Runtime that mounts Workspace paths via FUSE.
        msg = f"{self.__class__.__qualname__} paths are not path-like: {self}"
        raise NotImplementedError(msg)

    def as_posix(self) -> str:
        return str(self)

    def __str__(self) -> str:
        try:
            return self._str
        except AttributeError:
            self._str = (self._root + self.parser.sep.join(self._path_parts)) or "."
            return self._str

    def __bytes__(self) -> bytes:
        return str(self).encode("utf-8")

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({str(self)!r})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return str(self) == str(other)

    def __hash__(self) -> int:
        try:
            return self._hash
        except AttributeError:
            self._hash = hash(str(self))
            return self._hash

    @property
    def _parts(self) -> tuple[str, ...]:
        """Return a tuple that has the same natural ordering as paths of this type."""
        # Compatibility property (python <= 3.11), accessed via reverse equality comparison. This can't be avoided.
        return self._root, *self._path_parts

    _cparts = _parts

    @property
    def _str_normcase(self):
        # Compatibility property (python 3.12+), accessed via equality comparison. This can't be avoided.
        return str(self)

    @classmethod
    def _from_parts(cls, *args) -> NoReturn:
        # Compatibility method (python <= 3.11), accessed via reverse /-style building. This can't be avoided.
        # See __rtruediv__ for more information.
        raise TypeError("trigger NotImplemented")

    @property
    def _raw_paths(self) -> NoReturn:
        # Compatibility method (python 3.12+), accessed via reverse /-style building. This can't be avoided.
        # See __rtruediv__ for more information.
        raise TypeError("trigger NotImplemented")

    def __lt__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path_parts < other._path_parts

    def __le__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path_parts <= other._path_parts

    def __gt__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path_parts > other._path_parts

    def __ge__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path_parts >= other._path_parts

    def with_segments(self: P, *path_segments: bytes | str | os.PathLike) -> P:
        return type(self)(self._ws, *path_segments)

    @property
    def drive(self) -> str:
        return self._drv

    @property
    def root(self) -> str:
        return self._root

    @property
    def anchor(self) -> str:
        return self.drive + self.root

    @property
    def name(self) -> str:
        path_parts = self._path_parts
        return path_parts[-1] if path_parts else ""

    @property
    def parts(self) -> tuple[str, ...]:
        if self.drive or self.root:
            return self.drive + self.root, *self._path_parts
        return self._path_parts

    def with_name(self: P, name: str) -> P:
        parser = self.parser
        if not name or parser.sep in name or name == ".":
            msg = f"Invalid name: {name!r}"
            raise ValueError(msg)
        path_parts = list(self._path_parts)
        if not path_parts:
            raise ValueError(f"{self!r} has an empty name")
        path_parts[-1] = name
        return type(self)(self._ws, self.anchor, *path_parts)

    def with_suffix(self: P, suffix: str) -> P:
        stem = self.stem
        if not stem:
            msg = f"{self!r} has an empty name"
            raise ValueError(msg)
        if suffix and not suffix.startswith("."):
            msg = f"{self!r} invalid suffix: {suffix}"
            raise ValueError(msg)
        return self.with_name(stem + suffix)

    # pylint: disable=arguments-differ
    def relative_to(self: P, *other: str | bytes | os.PathLike, walk_up: bool = False) -> P:
        normalized = self.with_segments(*other)
        if self.anchor != normalized.anchor:
            msg = f"{str(self)!r} and {str(normalized)!r} have different anchors"
            raise ValueError(msg)
        path_parts0 = self._path_parts
        path_parts1 = normalized._path_parts  # pylint: disable=protected-access
        # Find the length of the common prefix.
        i = 0
        while i < len(path_parts0) and i < len(path_parts1) and path_parts0[i] == path_parts1[i]:
            i += 1
        relative_parts = path_parts0[i:]
        # Handle walking up.
        if i < len(path_parts1):
            if not walk_up:
                msg = f"{str(self)!r} is not in the subpath of {str(normalized)!r}"
                raise ValueError(msg)
            if ".." in path_parts1[i:]:
                raise ValueError(f"'..' segment in {str(normalized)!r} cannot be walked")
            walkup_parts = [".."] * (len(path_parts1) - i)
            relative_parts = (*walkup_parts, *relative_parts)
        return self.with_segments("", *relative_parts)

    def is_relative_to(self, *other: str | bytes | os.PathLike) -> bool:
        normalized = self.with_segments(*other)
        if self.anchor != normalized.anchor:
            return False
        path_parts0 = self._path_parts
        path_parts1 = normalized._path_parts  # pylint: disable=protected-access
        return path_parts0[: len(path_parts1)] == path_parts1

    @property
    def parent(self: P) -> P:
        rel_path = self._path_parts
        return self.with_segments(self.anchor, *rel_path[:-1]) if rel_path else self

    @property
    def parents(self: P) -> tuple[P, ...]:
        parents = []
        path = self
        parent = path.parent
        while path != parent:
            parents.append(parent)
            path = parent
            parent = path.parent
        return tuple(parents)

    def is_absolute(self) -> bool:
        return bool(self.anchor)

    def is_reserved(self) -> bool:
        return False

    def joinpath(self: P, *path_segments) -> P:
        return self.with_segments(self, *path_segments)

    def __truediv__(self: P, other: str | bytes | os.PathLike) -> P:
        try:
            return self.with_segments(*self._parts, other)
        except TypeError:
            return NotImplemented

    def __rtruediv__(self, other):
        # Note: this is only invoked if __truediv__ has already returned NotImplemented.
        # For the case of Path / _DatabricksPath this means the underlying __truediv__ is invoked.
        # The base-class implementations all access internals but yield NotImplemented if TypeError is raised. As
        # such we stub those internals (_from_parts and _raw_path) to trigger the NotImplemented path and ensure that
        # control ends up here.
        try:
            if isinstance(other, PurePath):
                return type(other)(other, *self._parts)
            return self.with_segments(other, *self._parts)
        except TypeError:
            return NotImplemented

    def match(self, path_pattern: str | bytes | os.PathLike, *, case_sensitive: bool | None = None) -> bool:
        # Convert the pattern to a fake path (with globs) to help with matching parts.
        if not isinstance(path_pattern, PurePath):
            path_pattern = self.with_segments(path_pattern)
        # Default to false if not specified.
        if case_sensitive is None:
            case_sensitive = True

        pattern_parts = path_pattern.parts
        if not pattern_parts:
            raise ValueError("empty pattern")
        # Short-circuit on situations where a match is logically impossible.
        path_parts = self.parts
        if len(path_parts) < len(pattern_parts) or len(path_parts) > len(pattern_parts) and path_pattern.anchor:
            return False
        # Check each part, starting from the end.
        for path_part, pattern_part in zip(reversed(path_parts), reversed(pattern_parts)):
            pattern = _PatternSelector.compile_pattern(pattern_part, case_sensitive=case_sensitive)
            if not pattern.match(path_part):
                return False
        return True

    def home(self):  # pylint: disable=arguments-differ
        """Return the user's home directory. Adapted from pathlib.Path"""
        return type(self)(self._ws, "~").expanduser()

    def expanduser(self: P) -> P:
        # Expand ~ (but NOT ~user) constructs.
        if not (self._drv or self._root) and self._path_parts and self._path_parts[0][:1] == "~":
            if self._path_parts[0] == "~":
                user_name = self._ws.current_user.me().user_name
            else:
                other_user = self._path_parts[0][1:]
                msg = f"Cannot determine home directory for: {other_user}"
                raise RuntimeError(msg)
            if user_name is None:
                raise RuntimeError("Could not determine home directory.")
            homedir = f"/Users/{user_name}"
            return self.with_segments(homedir, *self._path_parts[1:])
        return self

    def _return_false(self) -> bool:
        return False

    is_symlink = _return_false
    is_block_device = _return_false
    is_char_device = _return_false
    is_fifo = _return_false
    is_socket = _return_false
    is_mount = _return_false
    is_junction = _return_false

    def resolve(self: P, strict: bool = False) -> P:
        """Return the absolute path of the file or directory in Databricks Workspace."""
        absolute = self.absolute()
        if strict and not absolute.exists():
            msg = f"Path does not exist: {self}"
            raise FileNotFoundError(msg)
        # pylint: disable=protected-access
        return absolute._normalize()

    def _normalize(self: P) -> P:
        if ".." not in self._path_parts:
            return self
        segments: list[str] = []
        for part in self._path_parts:
            match part:
                case "..":
                    if segments:
                        segments.pop()
                case None | ".":
                    pass
                case _:
                    segments.append(part)
        # pylint: disable=protected-access
        return self.with_segments(self.anchor, *segments)._normalize()

    def absolute(self: P) -> P:
        if self.is_absolute():
            return self
        return self.with_segments(self.cwd(), self)

    def mkdir(self, mode: int = 0o600, parents: bool = True, exist_ok: bool = True) -> None:
        """Create a directory;Only mode 0o600 is supported."""
        if not exist_ok:
            raise ValueError("exist_ok must be True for Databricks Workspace")
        if not parents:
            raise ValueError("parents must be True for Databricks Workspace")
        if mode != 0o600:
            raise ValueError("other modes than 0o600 are not yet supported")
        self._mkdir()

    def _prepare_pattern(self, pattern: str | bytes | os.PathLike) -> Sequence[str]:
        if not pattern:
            raise ValueError("Glob pattern must not be empty.")
        parsed_pattern = self.with_segments(pattern)
        if parsed_pattern.anchor:
            msg = f"Non-relative patterns are unsupported: {pattern!s}"
            raise NotImplementedError(msg)
        pattern_parts = parsed_pattern._path_parts  # pylint: disable=protected-access
        if ".." in pattern_parts:
            msg = f"Parent traversal is not supported: {pattern!s}"
            raise ValueError(msg)
        if os.fspath(pattern)[-1] == self.parser.sep:
            pattern_parts = (*pattern_parts, "")
        return pattern_parts

    def glob(
        self: P,
        pattern: str | bytes | os.PathLike,
        *,
        case_sensitive: bool | None = None,
    ) -> Generator[P, None, None]:
        pattern_parts = self._prepare_pattern(pattern)
        if case_sensitive is None:
            case_sensitive = True
        selector = _Selector.parse(pattern_parts, case_sensitive=case_sensitive)
        yield from selector(self)

    def rglob(
        self: P,
        pattern: str | bytes | os.PathLike,
        *,
        case_sensitive: bool | None = None,
    ) -> Generator[P, None, None]:
        pattern_parts = ("**", *self._prepare_pattern(pattern))
        if case_sensitive is None:
            case_sensitive = True
        selector = _Selector.parse(pattern_parts, case_sensitive=case_sensitive)
        yield from selector(self)


class DBFSPath(_DatabricksPath):
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


class WorkspacePath(_DatabricksPath):
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
            return _BinaryUploadIO(self._ws, self.as_posix())
        if "r" in mode:
            with self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO) as f:
                data = f.read()
            return decode_with_bom(BytesIO(data), encoding=encoding, errors=errors, newline=newline)
        if "w" in mode:
            return _TextUploadIO(self._ws, self.as_posix())
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


T = TypeVar("T", bound="Path")


class _Selector(abc.ABC):
    @classmethod
    def parse(cls, pattern_parts: Sequence[str], *, case_sensitive: bool) -> _Selector:
        # The pattern language is:
        #  - '**' matches any number (including zero) of file or directory segments. Must be the entire segment.
        #  - '*' match any number of characters within a single segment.
        #  - '?' match a single character within a segment.
        #  - '[seq]' match a single character against the class (within a segment).
        #  - '[!seq]' negative match for a single character against the class (within a segment).
        #  - A trailing '/' (which presents here as a trailing empty segment) matches only directories.
        # There is no explicit escaping mechanism; literal matches against special characters above are possible as
        # character classes, for example: [*]
        #
        # Some sharp edges:
        #  - Multiple '**' segments are allowed.
        #  - Normally the '..' segment is allowed. (This can be used to match against siblings, for
        #    example: /home/bob/../jane/) However WorspacePath (and DBFS) do not support '..' traversal in paths.
        #  - Normally '.' is allowed, but eliminated before we reach this method.
        match pattern_parts:
            case ["**", *tail]:
                return _RecursivePatternSelector(tail, case_sensitive=case_sensitive)
            case [head, *tail] if case_sensitive and not _PatternSelector.needs_pattern(head):
                return _LiteralSelector(head, tail, case_sensitive=case_sensitive)
            case [head, *tail]:
                if "**" in head:
                    raise ValueError("Invalid pattern: '**' can only be a complete path component")
                return _PatternSelector(head, tail, case_sensitive=case_sensitive)
            case []:
                return _TerminalSelector()
        raise ValueError(f"Glob pattern unsupported: {pattern_parts}")

    @abstractmethod
    def __call__(self, path: T) -> Iterable[T]: ...


class _TerminalSelector(_Selector):
    def __call__(self, path: T) -> Iterable[T]:
        yield path


class _NonTerminalSelector(_Selector):
    __slots__ = (
        "_dir_only",
        "_child_selector",
    )

    def __init__(self, child_pattern_parts: Sequence[str], *, case_sensitive: bool) -> None:
        super().__init__()
        if child_pattern_parts:
            self._child_selector = self.parse(child_pattern_parts, case_sensitive=case_sensitive)
            self._dir_only = True
        else:
            self._child_selector = _TerminalSelector()
            self._dir_only = False

    def __call__(self, path: T) -> Iterable[T]:
        if path.is_dir():
            yield from self._select_children(path)

    @abstractmethod
    def _select_children(self, path: T) -> Iterable[T]: ...


class _LiteralSelector(_NonTerminalSelector):
    __slots__ = ("_literal_path",)

    def __init__(self, path: str, child_pattern_parts: Sequence[str], case_sensitive: bool) -> None:
        super().__init__(child_pattern_parts, case_sensitive=case_sensitive)
        self._literal_path = path

    def _select_children(self, path: T) -> Iterable[T]:
        candidate = path / self._literal_path
        if self._dir_only and candidate.is_dir() or candidate.exists():
            yield from self._child_selector(candidate)


class _PatternSelector(_NonTerminalSelector):
    __slots__ = ("_pattern",)

    # The special set of characters that indicate a glob pattern isn't a trivial literal.
    # Ref: https://docs.python.org/3/library/fnmatch.html#module-fnmatch
    _glob_specials = re.compile("[*?\\[\\]]")

    @classmethod
    def needs_pattern(cls, pattern: str) -> bool:
        return cls._glob_specials.search(pattern) is not None

    @classmethod
    def compile_pattern(cls, pattern: str, case_sensitive: bool) -> re.Pattern:
        flags = 0 if case_sensitive else re.IGNORECASE
        regex = fnmatch.translate(pattern)
        return re.compile(regex, flags=flags)

    def __init__(self, pattern: str, child_pattern_parts: Sequence[str], case_sensitive: bool) -> None:
        super().__init__(child_pattern_parts, case_sensitive=case_sensitive)
        self._pattern = self.compile_pattern(pattern, case_sensitive=case_sensitive)

    def _select_children(self, path: T) -> Iterable[T]:
        candidates = list(path.iterdir())
        for candidate in candidates:
            if self._dir_only and not candidate.is_dir():
                continue
            if self._pattern.match(candidate.name):
                yield from self._child_selector(candidate)


class _RecursivePatternSelector(_NonTerminalSelector):
    def __init__(self, child_pattern_parts: Sequence[str], case_sensitive: bool) -> None:
        super().__init__(child_pattern_parts, case_sensitive=case_sensitive)

    def _all_directories(self, path: T) -> Iterable[T]:
        # Depth-first traversal of directory tree, visiting this node first.
        yield path
        children = [child for child in path.iterdir() if child.is_dir()]
        for child in children:
            yield from self._all_directories(child)

    def _select_children(self, path: T) -> Iterable[T]:
        yielded = set()
        for starting_point in self._all_directories(path):
            for candidate in self._child_selector(starting_point):
                if candidate not in yielded:
                    yielded.add(candidate)
                    yield candidate


def _detect_encoding_bom(
    binary_io: BinaryIO, *, preserve_position: bool
) -> Literal["utf-32", "utf-16", "utf-8-sig"] | None:
    # Peek at the first (up to) 4 bytes, preserving the file position if requested.
    position = binary_io.tell() if preserve_position else None
    try:
        maybe_bom: bytes = binary_io.read(4)
    finally:
        if position is not None:
            binary_io.seek(position)
    # For these encodings, TextIOWrapper will skip over the BOM during decoding.
    if maybe_bom.startswith(codecs.BOM_UTF32_LE) or maybe_bom.startswith(codecs.BOM_UTF32_BE):
        return "utf-32"
    if maybe_bom.startswith(codecs.BOM_UTF16_LE) or maybe_bom.startswith(codecs.BOM_UTF16_BE):
        return "utf-16"
    if maybe_bom.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    return None


def decode_with_bom(
    file: BinaryIO, encoding: str | None = None, errors: str | None = None, newline: str | None = None
) -> TextIO:
    """Wrap an open binary file with a text decoder.

    The arguments have the same semantics as the built-in `open()` call, except that if the encoding is not specified
    and the file is seekable then it will be checked for a BOM. If a BOM marker is found, that encoding is used. When
    neither an encoding nor a BOM are present the encoding of the system locale is used.

    Args:
          file: the open (binary) file to wrap in text mode.
          encoding: force decoding with a specific locale. If not present the file BOM and system locale are used.
          errors: how decoding errors should be handled, as per open().
          newline: how newlines should be handled, as per open().
    Raises:
          ValueError: if the encoding should be detected via potential BOM marker but the file is not seekable.
    Returns:
          a text-based IO wrapper that will decode the underlying binary-mode file as text.
    """
    use_encoding = _detect_encoding_bom(file, preserve_position=True) if encoding is None else encoding
    return io.TextIOWrapper(file, encoding=use_encoding, errors=errors, newline=newline)


def _read_text_from_binary_io(binary_io: BinaryIO, size: int = -1) -> str:
    with decode_with_bom(binary_io) as f:
        return f.read(size)


def read_text(path: Path, size: int = -1) -> str:
    """Read a file as text, decoding according to the BOM marker if that is present.

    This differs to the normal `.read_text()` method on path which does not support BOM markers.

    Arguments:
        path: the path to read text from.
        size: how much text (measured in characters) to read. If negative, all text is read. Less may be read if the
            file is smaller than the specified size.
    Returns:
        The string content of the file, up to the specified size.
    """
    with path.open("rb") as binary_io:
        # If the open file is seekable, we can detect the BOM and decode without re-opening.
        if binary_io.seekable():
            return _read_text_from_binary_io(binary_io, size=size)
        # If non-seekable, we can't rewind so we have to slurp it and do it from memory.
        if size != -1:
            msg = "Cannot specify read size for a non-seekable file"
            raise ValueError(msg)
        binary_content = binary_io.read()
    with io.BytesIO(binary_content) as binary_io:
        return _read_text_from_binary_io(binary_io, size=size)
