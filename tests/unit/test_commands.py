from unittest.mock import create_autospec

import pytest
from databricks.sdk.errors import DatabricksError
from databricks.sdk.mixins.compute import ClustersExt
from databricks.sdk.service import compute

from databricks.labs.blueprint.commands import CommandExecutor, _ReturnToPrintJson


def test_parse_results_data_as_json():
    out, has_return = _ReturnToPrintJson.transform("\n".join(["return [{'success': True}]"]))
    assert out.splitlines() == ["import json", "print(json.dumps([{'success': True}]))"]
    assert has_return


def test_parse_results_data_as_json_with_json_import_existing():
    out, has_return = _ReturnToPrintJson.transform("\n".join(["import json", "return [{'success': True}]"]))
    assert out.splitlines() == ["import json", "print(json.dumps([{'success': True}]))"]
    assert has_return


def test_parse_results_data_as_json_with_json_without_return():
    out, has_return = _ReturnToPrintJson.transform("\n".join(["print(1)"]))
    assert out.splitlines() == ["print(1)"]
    assert not has_return


def test_parse_results_data_as_json_with_json_print_and_return():
    with pytest.raises(ValueError):
        _ReturnToPrintJson.transform("\n".join(["print(1)", "return 1"]))


def test_fails_execution():
    clusters_ext = create_autospec(ClustersExt)
    command_execution = create_autospec(compute.CommandExecutionAPI)
    command_executor = CommandExecutor(clusters_ext, command_execution, lambda: "foo")

    command_execution.execute().result().results.summary = "abc"

    with pytest.raises(DatabricksError):
        command_executor.run("return 1")
