import inspect
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import databricks.labs.blueprint.entrypoint as entrypoint
from databricks.labs.blueprint.entrypoint import (
    find_project_root,
    get_logger,
    relative_paths,
    run_main,
)
from databricks.labs.blueprint.wheels import ProductInfo


def test_common_paths_in_root():
    a, b = relative_paths("/etc/hosts", "/usr/bin/python3")
    assert a.as_posix() == "/etc/hosts"
    assert b.as_posix() == "/usr/bin/python3"


def test_common_paths_in_subdir():
    a, b = relative_paths(__file__, inspect.getfile(ProductInfo))
    assert a.as_posix() == "tests/unit/test_entrypoint.py"
    assert b.as_posix() == "src/databricks/labs/blueprint/wheels.py"


def test_find_project_root():
    root = find_project_root(__file__)

    this_file = Path(__file__)
    assert this_file.parent.parent.parent == root


def test_find_project_root_in_tmp_dir_fails(tmp_path):
    with pytest.raises(NotADirectoryError):
        find_project_root((tmp_path / "foo.py").as_posix())


def test_run_main():
    main = MagicMock()
    run_main(main)
    main.assert_called_once()


def test_get_logger_name() -> None:
    """Ensure the logger name is set to something module-like, even if the file is a script/path."""
    # File is something like /path/to/blueprint/tests/unit/test_entrypoint.py
    logger = get_logger(__file__)

    assert logger.name == "tests.unit.test_entrypoint"


@pytest.fixture
def log_manager() -> logging.Manager:
    """Logging manager, independent of the system logging."""
    root = logging.RootLogger(logging.WARNING)
    return logging.Manager(root)


def test_get_logger_when_in_debug(monkeypatch, log_manager: logging.Manager) -> None:
    """When in debug mode, the logger is hardcoded to DEBUG level."""
    monkeypatch.setattr(entrypoint, "is_in_debug", lambda: True)

    # Ensure we don't get a cached logger that has already been configured to a level.
    logger = get_logger(__file__, manager=log_manager)

    assert logger.level == logging.DEBUG
    assert logger.propagate is True


def test_get_logger_when_not_in_debug(monkeypatch, log_manager: logging.Manager) -> None:
    """When in not in debug mode, the logger should simply propagate to the parent."""
    monkeypatch.setattr(entrypoint, "is_in_debug", lambda: False)

    # Ensure we don't get a cached logger that has already been configured to a level.
    logger = get_logger(__file__, manager=log_manager)

    assert logger.level == logging.NOTSET
    assert logger.propagate is True
