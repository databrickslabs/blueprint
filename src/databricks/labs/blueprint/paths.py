from __future__ import annotations

import abc
import fnmatch
import locale
import logging
import os
import posixpath
import re
import sys
from io import BytesIO, StringIO
from pathlib import Path, PurePath
from typing import NoReturn
from urllib.parse import quote_from_bytes as urlquote_from_bytes

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.service.workspace import (
    ExportFormat,
    ImportFormat,
    Language,
    ObjectInfo,
    ObjectType,
)

logger = logging.getLogger(__name__)


class _DatabricksFlavour:
    # adapted from pathlib._Flavour, where we ignore support for drives, as we
    # don't have that concept in Databricks. We also ignore support for Windows
    # paths, as we only support POSIX paths in Databricks.

    sep = "/"
    altsep = ""
    has_drv = False
    pathmod = posixpath
    is_supported = True

    def __init__(self, ws: WorkspaceClient):
        self.join = self.sep.join
        self._ws = ws

    def parse_parts(self, parts: list[str]) -> tuple[str, str, list[str]]:
        # adapted from pathlib._Flavour.parse_parts,
        # where we ignore support for drives, as we
        # don't have that concept in Databricks
        parsed = []
        drv = root = ""
        for part in reversed(parts):
            if not part:
                continue
            drv, root, rel = self.splitroot(part)
            if self.sep not in rel:
                if rel and rel != ".":
                    parsed.append(sys.intern(rel))
                continue
            for part_ in reversed(rel.split(self.sep)):
                if part_ and part_ != ".":
                    parsed.append(sys.intern(part_))
        if drv or root:
            parsed.append(drv + root)
        parsed.reverse()
        return drv, root, parsed

    @staticmethod
    def join_parsed_parts(
        drv: str,
        root: str,
        parts: list[str],
        _,
        root2: str,
        parts2: list[str],
    ) -> tuple[str, str, list[str]]:
        # adapted from pathlib.PurePosixPath, where we ignore support for drives,
        # as we don't have that concept in Databricks
        if root2:
            return drv, root2, [drv + root2] + parts2[1:]
        return drv, root, parts + parts2

    @staticmethod
    def splitroot(part, sep=sep) -> tuple[str, str, str]:
        if part and part[0] == sep:
            stripped_part = part.lstrip(sep)
            if len(part) - len(stripped_part) == 2:
                return "", sep * 2, stripped_part
            return "", sep, stripped_part
        return "", "", part

    @staticmethod
    def casefold(value: str) -> str:
        return value

    @staticmethod
    def casefold_parts(parts: list[str]) -> list[str]:
        return parts

    @staticmethod
    def compile_pattern(pattern: str):
        return re.compile(fnmatch.translate(pattern)).fullmatch

    @staticmethod
    def is_reserved(_) -> bool:
        return False

    def make_uri(self, path) -> str:
        return self._ws.config.host + "#workspace" + urlquote_from_bytes(bytes(path))

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self._ws}>"


def _na(fn: str):
    def _inner(*_, **__):
        __tracebackhide__ = True  # pylint: disable=unused-variable
        raise NotImplementedError(f"{fn}() is not available for Databricks Workspace")

    return _inner


class _ScandirItem:
    def __init__(self, object_info):
        self._object_info = object_info

    def __fspath__(self):
        return self._object_info.path

    def is_dir(self, follow_symlinks=False):  # pylint: disable=unused-argument
        # follow_symlinks is for compatibility with Python 3.11
        return self._object_info.object_type == ObjectType.DIRECTORY

    def is_file(self, follow_symlinks=False):  # pylint: disable=unused-argument
        # follow_symlinks is for compatibility with Python 3.11
        # TODO: check if we want to show notebooks as files
        return self._object_info.object_type == ObjectType.FILE

    def is_symlink(self):
        return False

    @property
    def name(self):
        return os.path.basename(self._object_info.path)


class _ScandirIterator:
    def __init__(self, objects):
        self._it = objects

    def __iter__(self):
        for object_info in self._it:
            yield _ScandirItem(object_info)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class _DatabricksAccessor:
    chmod = _na("accessor.chmod")
    getcwd = _na("accessor.getcwd")
    group = _na("accessor.group")
    link = _na("accessor.link")
    mkdir = _na("accessor.mkdir")
    owner = _na("accessor.owner")
    readlink = _na("accessor.readlink")
    realpath = _na("accessor.realpath")
    rename = _na("accessor.rename")
    replace = _na("accessor.replace")
    rmdir = _na("accessor.rmdir")
    stat = _na("accessor.stat")
    symlink = _na("accessor.symlink")
    unlink = _na("accessor.unlink")

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self._ws}>"

    def scandir(self, path):
        objects = self._ws.workspace.list(path)
        return _ScandirIterator(objects)

    def listdir(self, path):
        return [item.name for item in self.scandir(path)]


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


class WorkspacePath(Path):  # pylint: disable=too-many-public-methods
    """Experimental implementation of pathlib.Path for Databricks Workspace."""

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
    #
    # This implementation for Workspace paths does the following:
    #     1. Flavour is basically posix-style, with the caveat that we don't bother with the special //-prefix handling.
    #     2. The Accessor is delegated to existing routines available via the workspace client.
    #     3. Python 3.12 introduces some new API elements. Because these are source-compatible with earlier versions
    #        these are forward-ported and implemented.
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
        # The cached _object_info value for the instance.
        "_cached_object_info",
    )
    _cached_object_info: ObjectInfo

    _SUFFIXES = {".py": Language.PYTHON, ".sql": Language.SQL, ".scala": Language.SCALA, ".R": Language.R}

    # Path semantics are posix-like.
    parser = posixpath

    # Compatibility attribute, for when superclass implementations get invoked on python <= 3.11.
    _flavour = object()

    cwd = _na("cwd")
    stat = _na("stat")
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

    def __new__(cls, *args, **kwargs) -> WorkspacePath:
        # Force all initialisation to go via __init__() irrespective of the (Python-specific) base version.
        return object.__new__(cls)

    def __init__(self, ws: WorkspaceClient, *args) -> None:  # pylint: disable=super-init-not-called,useless-suppression
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
    def _to_raw_paths(*args) -> list[str]:
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

    def __reduce__(self) -> NoReturn:
        # Cannot support pickling because we can't pickle the workspace client.
        msg = "Pickling Workspace paths is not supported."
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
        msg = f"Workspace paths are not path-like: {self}"
        raise NotImplementedError(msg)

    def as_posix(self):
        return str(self)

    def __str__(self):
        try:
            return self._str
        except AttributeError:
            self._str = (self._root + self.parser.sep.join(self._path_parts)) or "."
            return self._str

    def __bytes__(self):
        return str(self).encode("utf-8")

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self)!r})"

    def as_uri(self) -> str:
        return f"{self._ws.config.host}#workspace{urlquote_from_bytes(bytes(self))}"

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return str(self) == str(other)

    def __hash__(self):
        try:
            return self._hash
        except AttributeError:
            self._hash = hash(str(self))
            return self._hash

    def _parts(self) -> tuple[str, ...]:
        """Return a tuple that has the same natural ordering as paths of this type."""
        return self._root, *self._path_parts

    @property
    def _cparts(self):
        # Compatibility property (python <= 3.11), accessed via reverse equality comparison. This can't be avoided.
        return self._parts()

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

    def __lt__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path_parts < other._path_parts

    def __le__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path_parts <= other._path_parts

    def __gt__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path_parts > other._path_parts

    def __ge__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path_parts >= other._path_parts

    def with_segments(self, *pathsegments):
        return type(self)(self._ws, *pathsegments)

    @property
    def drive(self) -> str:
        return self._drv

    @property
    def root(self):
        return self._root

    @property
    def anchor(self):
        return self.drive + self.root

    @property
    def name(self):
        path_parts = self._path_parts
        return path_parts[-1] if path_parts else ""

    @property
    def parts(self):
        if self.drive or self.root:
            return self.drive + self.root, *self._path_parts
        return self._path_parts

    def with_name(self, name):
        parser = self.parser
        if not name or parser.sep in name or name == ".":
            msg = f"Invalid name: {name!r}"
            raise ValueError(msg)
        path_parts = list(self._path_parts)
        if not path_parts:
            raise ValueError(f"{self!r} has an empty name")
        path_parts[-1] = name
        return type(self)(self._ws, self.anchor, *path_parts)

    def with_suffix(self, suffix):
        stem = self.stem
        if not stem:
            msg = f"{self!r} has an empty name"
            raise ValueError(msg)
        if suffix and not suffix.startswith("."):
            msg = f"{self!r} invalid suffix: {suffix}"
            raise ValueError(msg)
        return self.with_name(stem + suffix)

    def relative_to(self, other, *more_other, walk_up=False):  # pylint: disable=arguments-differ
        other = self.with_segments(other, *more_other)
        if self.anchor != other.anchor:
            msg = f"{str(self)!r} and {str(other)!r} have different anchors"
            raise ValueError(msg)
        path_parts0 = self._path_parts
        path_parts1 = other._path_parts  # pylint: disable=protected-access
        # Find the length of the common prefix.
        i = 0
        while i < len(path_parts0) and i < len(path_parts1) and path_parts0[i] == path_parts1[i]:
            i += 1
        relative_parts = path_parts0[i:]
        # Handle walking up.
        if i < len(path_parts1):
            if not walk_up:
                msg = f"{str(self)!r} is not in the subpath of {str(other)!r}"
                raise ValueError(msg)
            if ".." in path_parts1[i:]:
                raise ValueError(f"'..' segment in {str(other)!r} cannot be walked")
            walkup_parts = [".."] * (len(path_parts1) - i)
            relative_parts = (*walkup_parts, *relative_parts)
        return self.with_segments("", *relative_parts)

    def is_relative_to(self, other, *more_other):  # pylint: disable=arguments-differ
        other = self.with_segments(other, *more_other)
        if self.anchor != other.anchor:
            return False
        path_parts0 = self._path_parts
        path_parts1 = other._path_parts  # pylint: disable=protected-access
        return path_parts0[: len(path_parts1)] == path_parts1

    @property
    def parent(self):
        rel_path = self._path_parts
        return self.with_segments(self.anchor, *rel_path[:-1]) if rel_path else self

    @property
    def parents(self):
        parents = []
        path = self
        parent = path.parent
        while path != parent:
            parents.append(parent)
            path = parent
            parent = path.parent
        return tuple(parents)

    def is_absolute(self):
        return bool(self.anchor)

    def is_reserved(self):
        return False

    def joinpath(self, *pathsegments):
        return self.with_segments(self, *pathsegments)

    def __truediv__(self, other):
        try:
            return self.with_segments(*self._parts(), other)
        except TypeError:
            return NotImplemented

    def __rtruediv__(self, other):
        # Note: this is only invoked if __truediv__ has already returned NotImplemented.
        # For the case of Path / WorkspacePath this means the underlying __truediv__ is invoked.
        # The base-class implementations all access internals but yield NotImplemented if TypeError is raised. As
        # such we stub those internals (_from_parts and _raw_path) to trigger the NotImplemented path and ensure that
        # control ends up here.
        try:
            if isinstance(other, PurePath):
                return type(other)(other, *self._parts())
            return self.with_segments(other, *self._parts())
        except TypeError:
            return NotImplemented

    @classmethod
    def _compile_pattern(cls, pattern: str, case_sensitive: bool) -> re.Pattern:
        flags = 0 if case_sensitive else re.IGNORECASE
        regex = fnmatch.translate(pattern)
        return re.compile(regex, flags=flags)

    def match(self, path_pattern, *, case_sensitive=None):
        # Convert the pattern to a fake path (with globs) to help with matching parts.
        if not isinstance(path_pattern, PurePath):
            path_pattern = self.with_segments(path_pattern)
        # Default to false if not specified.
        if case_sensitive is None:
            case_sensitive = True
        # Reverse the parts.
        path_parts = self.parts
        pattern_parts = path_pattern.parts
        # Error
        if not pattern_parts:
            raise ValueError("empty pattern")
        # Impossible matches.
        if len(path_parts) < len(pattern_parts) or len(path_parts) > len(pattern_parts) and path_pattern.anchor:
            return False
        # Check each part.
        for path_part, pattern_part in zip(reversed(path_parts), reversed(pattern_parts)):
            pattern = self._compile_pattern(pattern_part, case_sensitive=case_sensitive)
            if not pattern.match(path_part):
                return False
        return True

    def as_fuse(self):
        """Return FUSE-mounted path in Databricks Runtime."""
        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            logger.warning("This method is only available in Databricks Runtime")
        return Path("/Workspace", self.as_posix().lstrip("/"))

    def home(self):  # pylint: disable=arguments-differ
        """Return the user's home directory. Adapted from pathlib.Path"""
        return WorkspacePath(self._ws, "~").expanduser()

    def exists(self, *, follow_symlinks=True):
        """Return True if the path points to an existing file, directory, or notebook"""
        if not follow_symlinks:
            raise NotImplementedError("follow_symlinks=False is not supported for Databricks Workspace")
        try:
            self._ws.workspace.get_status(self.as_posix())
            return True
        except NotFound:
            return False

    def mkdir(self, mode=0o600, parents=True, exist_ok=True):
        """Create a directory in Databricks Workspace. Only mode 0o600 is supported."""
        if not exist_ok:
            raise ValueError("exist_ok must be True for Databricks Workspace")
        if not parents:
            raise ValueError("parents must be True for Databricks Workspace")
        if mode != 0o600:
            raise ValueError("other modes than 0o600 are not yet supported")
        self._ws.workspace.mkdirs(self.as_posix())

    def rmdir(self, recursive=False):
        """Remove a directory in Databricks Workspace"""
        self._ws.workspace.delete(self.as_posix(), recursive=recursive)

    def rename(self, target, overwrite=False):
        """Rename a file or directory in Databricks Workspace"""
        dst = WorkspacePath(self._ws, target)
        with self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO) as f:
            self._ws.workspace.upload(dst.as_posix(), f.read(), format=ImportFormat.AUTO, overwrite=overwrite)
        self.unlink()

    def replace(self, target):
        """Rename a file or directory in Databricks Workspace, overwriting the target if it exists."""
        return self.rename(target, overwrite=True)

    def unlink(self, missing_ok=False):
        """Remove a file in Databricks Workspace."""
        if not missing_ok and not self.exists():
            raise FileNotFoundError(f"{self.as_posix()} does not exist")
        self._ws.workspace.delete(self.as_posix())

    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        """Open a file in Databricks Workspace. Only text and binary modes are supported."""
        if encoding is None or encoding == "locale":
            encoding = locale.getpreferredencoding(False)
        if "b" in mode and "r" in mode:
            return self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO)
        if "b" in mode and "w" in mode:
            return _BinaryUploadIO(self._ws, self.as_posix())
        if "r" in mode:
            with self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO) as f:
                return StringIO(f.read().decode(encoding))
        if "w" in mode:
            return _TextUploadIO(self._ws, self.as_posix())
        raise ValueError(f"invalid mode: {mode}")

    @property
    def suffix(self):
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

    def _return_false(self) -> bool:
        return False

    is_symlink = _return_false
    is_block_device = _return_false
    is_char_device = _return_false
    is_fifo = _return_false
    is_socket = _return_false
    is_mount = _return_false
    is_junction = _return_false

    def resolve(self, strict=False):
        """Return the absolute path of the file or directory in Databricks Workspace."""
        return self

    def absolute(self):
        if self.is_absolute():
            return self
        return self.with_segments(self.cwd(), self)

    def is_dir(self):
        """Return True if the path points to a directory in Databricks Workspace."""
        try:
            return self._object_info.object_type == ObjectType.DIRECTORY
        except DatabricksError:
            return False

    def is_file(self):
        """Return True if the path points to a file in Databricks Workspace."""
        try:
            return self._object_info.object_type == ObjectType.FILE
        except DatabricksError:
            return False

    def _scandir(self):
        # TODO: Not yet invoked; work-in-progress.
        objects = self._ws.workspace.list(self.as_posix())
        return _ScandirIterator(objects)

    def expanduser(self):
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

    def is_notebook(self):
        """Return True if the path points to a notebook in Databricks Workspace."""
        try:
            return self._object_info.object_type == ObjectType.NOTEBOOK
        except DatabricksError:
            return False
