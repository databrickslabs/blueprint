import sys
from pathlib import Path

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import (
    find_project_root,
    get_logger,
    relative_paths,
)
from databricks.labs.blueprint.tui import Prompts

blueprint = App(__file__)
logger = get_logger(__file__)

MAIN_PY_FILE = '''from databricks.sdk import AccountClient, WorkspaceClient
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.cli import App

__app__ = App(__file__)
logger = get_logger(__file__)


@__app__.command
def me(w: WorkspaceClient, greeting: str):
    """Shows current username"""
    logger.info(f"{greeting}, {w.current_user.me().user_name}!")


@__app__.command(is_account=True)
def workspaces(a: AccountClient):
    """Shows workspaces"""
    for ws in a.workspaces.list():
        logger.info(f"Workspace: {ws.workspace_name} ({ws.workspace_id})")


if "__main__" == __name__:
    __app__()

'''

LABS_YML_FILE = """---
name: __app__
description: Common libraries for Databricks Labs
install:
  script: src/databricks/labs/__app__/__init__.py
entrypoint: src/databricks/labs/__app__/__main__.py
min_python: 3.10
  - name: me
    description: shows current username
    flags:
     - name: greeting
       default: Hello
       description: Greeting prefix
  - name: workspaces
    is_account: true
    description: shows current workspaces
"""


@blueprint.command(is_unauthenticated=True)
def init_project(target):
    """Creates the required boilerplate structure"""
    prompts = Prompts()

    project_root = find_project_root(__file__)
    target_folder = Path(target)

    project_name = prompts.question("Name of the project", default=target_folder.name)
    src_dir, dst_dir = relative_paths(project_root, target_folder.absolute())

    ignore_names = {
        ".git",
        ".venv",
        ".databricks",
        ".mypy_cache",
        ".idea",
        ".coverage",
        "htmlcov",
        "__pycache__",
        "tests",
        ".databricks-login.json",
        "coverage.xml",
        "dist",
        "docs",
    }
    queue: list[Path] = [src_dir]  # type: ignore[annotation-unchecked]
    while queue:
        current = queue.pop(0)
        if current.name in ignore_names:
            continue
        if current.is_file():
            relative_file_name = current.as_posix().replace("blueprint", project_name)
            dst_file = dst_dir / relative_file_name
            dst_file.parent.mkdir(exist_ok=True, parents=True)
            with current.open("r", encoding=sys.getdefaultencoding()) as src, dst_file.open("w") as dst:
                content = src.read().replace("blueprint", project_name)
                content = content.replace("databricks-sdk", "databricks-labs-blueprint")
                dst.write(content)
            continue
        virtual_env_marker = current / "pyvenv.cfg"
        if virtual_env_marker.exists():
            continue
        for file in current.iterdir():
            if file.as_posix() == "src/databricks/labs/blueprint":
                continue
            queue.append(file)
    inner_package_dir = dst_dir / "src" / "databricks" / "labs" / project_name
    inner_package_dir.mkdir(parents=True, exist_ok=True)
    with (inner_package_dir / "__main__.py").open("w") as f:
        f.write(MAIN_PY_FILE.replace("__app__", project_name))
    with (inner_package_dir / "__init__.py").open("w") as f:
        f.write(f"from databricks.labs.{project_name}.__about__ import __version__")
    with (inner_package_dir / "__about__.py").open("w") as f:
        f.write('__version__ = "0.0.0"\n')
    with (dst_dir / "labs.yml").open("w") as f:
        f.write(LABS_YML_FILE.replace("__app__", project_name))
    with (dst_dir / "CODEOWNERS").open("w") as f:
        f.write(f"* @nfx\n/src @databrickslabs/{project_name}-write\n/tests @databrickslabs/{project_name}-write\n")
    with (dst_dir / "CHANGELOG.md").open("w") as f:
        f.write(f"# Version changelog\n\n## 0.0.0\n\nInitial {project_name} commit\n")


if __name__ == "__main__":
    blueprint()
