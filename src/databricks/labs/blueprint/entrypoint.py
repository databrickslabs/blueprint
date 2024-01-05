import logging
import os
import sys
from pathlib import Path
from typing import Any

from databricks.labs.blueprint.logger import install_logger


def get_logger(file_name: str):
    """Used as `get_logger(__file__)` to return a relevant logger for a file

    :param file_name: str: use __file__ special constant

    """
    project_root = find_project_root().absolute()
    entrypoint = Path(file_name).absolute()

    relative = entrypoint.relative_to(project_root).as_posix()
    relative = relative.removeprefix("src" + os.sep)
    relative = relative.removesuffix("/__main__.py")
    relative = relative.removesuffix("/__init__.py")
    relative = relative.removesuffix("/cli.py")
    relative = relative.removesuffix(".py")
    module_name = relative.replace(os.sep, ".")

    logger = logging.getLogger(module_name)

    level = "INFO"
    if is_in_debug():
        level = "DEBUG"
    logger.setLevel(level)

    return logger


def run_main(main):
    """Runs main function with a logger

    :param main: function that takes command-line arguments

    """
    install_logger()
    main(*sys.argv[1:])


def find_project_root(current: Any = None) -> Path:
    """Returns pathlib.Path for the nearest folder with pyproject.toml or setup.py file"""
    this_path = Path.cwd() if current is None else Path(current)
    # TODO: detect when in wheel
    for leaf in ["pyproject.toml", "setup.py"]:
        root = find_dir_with_leaf(this_path, leaf)
        if root is not None:
            return root
    msg = "Cannot find project root"
    raise NotADirectoryError(msg)


def find_dir_with_leaf(folder: Path, leaf: str) -> Path | None:
    """Returns path object for the nearest folder with a leaf file

    :param folder: Path: starting location
    :param leaf: str: name of the file or folder

    """
    root = folder.root
    while str(folder.absolute()) != root:
        if (folder / leaf).exists():
            return folder
        folder = folder.parent
    return None


def is_in_debug() -> bool:
    """Returns true if run from VSCode or IntelliJ"""
    if "IDE_PROJECT_ROOTS" in os.environ:
        return True
    return os.path.basename(sys.argv[0]) in ["_jb_pytest_runner.py", "testlauncher.py"]


def relative_paths(*maybe_paths) -> list[Path]:
    """Converts list of paths to relative path objects

    :param *maybe_paths: string-like arguments

    """
    all_paths = [Path(str(_)) for _ in maybe_paths]
    common_path = Path(os.path.commonpath([_.as_posix() for _ in all_paths]))
    return [_.relative_to(common_path) for _ in all_paths]
