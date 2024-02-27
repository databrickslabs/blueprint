import ast
import html
import json
import logging
import platform
import re
import sys
import threading
from collections.abc import Callable
from typing import Any

from databricks.sdk.core import DatabricksError
from databricks.sdk.mixins.compute import ClustersExt
from databricks.sdk.service import compute

_out_re = re.compile(r"Out\[[\d\s]+]:\s")
_tag_re = re.compile(r"<[^>]*>")
_exception_re = re.compile(r".*Exception:\s+(.*)")
_execution_error_re = re.compile(r"ExecutionError: ([\s\S]*)\n(StatusCode=[0-9]*)\n(StatusDescription=.*)\n")
_error_message_re = re.compile(r"ErrorMessage=(.+)\n")
_ascii_escape_re = re.compile(r"(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]")
_LOG = logging.getLogger("databricks.sdk")


class _ReturnToPrintJson(ast.NodeTransformer):
    def __init__(self) -> None:
        self._has_json_import = False
        self._has_print = False
        self.has_return = False

    @staticmethod
    def transform(code: str) -> tuple[str, bool]:
        major, minor, _ = platform.python_version_tuple()
        unsupported_version = (int(major), int(minor)) < (3, 8)
        has_return_in_last_line = code.splitlines()[-1].startswith("return ")

        if unsupported_version and not has_return_in_last_line:
            return code, False

        if unsupported_version and has_return_in_last_line:
            raise ValueError(
                "dynamic conversion of return .. to print(json.dumps(..)) is only possible starting from Python 3.8"
            )

        # perform AST transformations for very repetitive tasks, like JSON serialization
        code_tree = ast.parse(code)
        transform = _ReturnToPrintJson()
        new_tree = transform.apply(code_tree)
        code = ast.unparse(new_tree)
        return code, transform.has_return

    def apply(self, raw_node: ast.AST) -> ast.AST:
        node: ast.stmt = self.visit(raw_node)
        if self.has_return and self._has_print:
            msg = "cannot have print() call, return .. is converted to print(json.dumps(..))"
            raise ValueError(msg)
        if self.has_return and not self._has_json_import:
            new_import: ast.stmt = ast.parse("import json").body[0]
            node.body.insert(0, new_import)  # type: ignore[attr-defined]
        return node

    def visit_Import(self, node: ast.Import) -> ast.Import:  # noqa: N802
        for name in node.names:
            if ast.unparse(name) != "json":
                continue
            self._has_json_import = True
            break
        return node

    def visit_Call(self, node: ast.Call):  # noqa: N802
        if ast.unparse(node.func) == "print":
            self._has_print = True
        return node

    def visit_Return(self, node):  # noqa: N802
        value = node.value
        if not value:
            # Remove the original return statement
            return None
        return_expr = ast.unparse(value)
        replaced_code = f"print(json.dumps({return_expr}))"
        print_call = ast.parse(replaced_code).body[0]
        self.has_return = True
        return print_call


class CommandExecutor:
    def __init__(
        self,
        clusters: ClustersExt,
        command_execution: compute.CommandExecutionAPI,
        cluster_id_provider: Callable[[], str],
        *,
        language: compute.Language = compute.Language.PYTHON,
    ):
        self._cluster_id_provider = cluster_id_provider
        self._language = language
        self._clusters = clusters
        self._commands = command_execution
        self._lock = threading.Lock()
        self._ctx: compute.ContextStatusResponse | None = None

    def run(self, code: str, *, result_as_json=False, detect_return=True) -> Any:
        code = self._trim_leading_whitespace(code)

        if self._language == compute.Language.PYTHON and detect_return and not result_as_json:
            code, result_as_json = _ReturnToPrintJson.transform(code)

        ctx = self._running_command_context()
        cluster_id = self._cluster_id_provider()
        command_status_response = self._commands.execute(
            cluster_id=cluster_id, language=self._language, context_id=ctx.id, command=code
        ).result()

        results = command_status_response.results
        assert results is not None
        if command_status_response.status == compute.CommandStatus.FINISHED:
            self._raise_if_failed(results)
            if results.result_type == compute.ResultType.TEXT and result_as_json:
                try:
                    # parse json from converted return statement
                    assert results.data is not None
                    return json.loads(results.data)
                except json.JSONDecodeError as e:
                    _LOG.warning("cannot parse converted return statement. Just returning text", exc_info=e)
                    return results.data
            return results.data
        # there might be an opportunity to convert builtin exceptions
        assert results.summary is not None
        raise DatabricksError(results.summary)

    def install_notebook_library(self, library: str):
        return self.run(
            f"""
            get_ipython().run_line_magic('pip', 'install {library}')
            dbutils.library.restartPython()
            """
        )

    def _running_command_context(self) -> compute.ContextStatusResponse:
        if self._ctx:
            return self._ctx
        with self._lock:
            if self._ctx:
                return self._ctx
            cluster_id = self._cluster_id_provider()
            self._clusters.ensure_cluster_is_running(cluster_id)
            self._ctx = self._commands.create(cluster_id=cluster_id, language=self._language).result()
        return self._ctx

    @staticmethod
    def _is_failed(results: compute.Results) -> bool:
        return results.result_type == compute.ResultType.ERROR

    @staticmethod
    def _text(results: compute.Results) -> str:
        if results.result_type != compute.ResultType.TEXT:
            return ""
        return _out_re.sub("", str(results.data))

    def _raise_if_failed(self, results: compute.Results):
        if not self._is_failed(results):
            return
        raise DatabricksError(self._error_from_results(results))

    def _error_from_results(self, results: compute.Results):
        """Converts results into an error

        :param results: compute.Results:

        """
        if not self._is_failed(results):
            return None
        results_cause = results.cause
        if results_cause:
            sys.stderr.write(_ascii_escape_re.sub("", results_cause))
        else:
            results_cause = ""

        summary = ""
        if results.summary:
            summary = results.summary
        summary = _tag_re.sub("", summary)
        summary = html.unescape(summary)

        exception_matches = _exception_re.findall(summary)
        if len(exception_matches) == 1:
            summary = exception_matches[0].replace("; nested exception is:", "")
            summary = summary.rstrip(" ")
            return summary

        execution_error_matches = _execution_error_re.findall(results_cause)
        if len(execution_error_matches) == 1:
            return "\n".join(execution_error_matches[0])

        error_message_matches = _error_message_re.findall(results_cause)
        if len(error_message_matches) == 1:
            return error_message_matches[0]

        return summary

    @staticmethod
    def _trim_leading_whitespace(command_str: str) -> str:
        """Removes leading whitespace, so that Python code blocks that
        are embedded into Python code still could be interpreted properly."""
        lines = command_str.replace("\t", "    ").split("\n")
        leading_whitespace = sys.maxsize
        if lines[0] == "":
            lines = lines[1:]
        for line in lines:
            pos = 0
            for char in line:
                if char in {" ", "\t"}:
                    pos += 1
                else:
                    break
            leading_whitespace = min(leading_whitespace, pos)
        new_command = ""
        for line in lines:
            if line == "" or line.strip(" \t") == "":
                continue
            if len(line) < leading_whitespace:
                new_command += line + "\n"
            else:
                new_command += line[leading_whitespace:] + "\n"
        return new_command.strip()
