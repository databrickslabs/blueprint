import inspect
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from databricks.labs.blueprint.entrypoint import (
    find_project_root,
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
