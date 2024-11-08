"""Databricks' path specification, in the same vein as :module:`posixpath` and :module:`ntpath`.

Paths are Posix-like, but we don't use the builtin :module:`posixpath` directly because as of Python 3.13 the module
itself is part of a path's identity for the purposes of comparison.
"""

from posixpath import join, sep

__all__ = ("join", "sep")
