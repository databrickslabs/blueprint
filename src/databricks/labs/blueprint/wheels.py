import datetime
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.mixins.compute import SemVer
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.blueprint.entrypoint import find_project_root
from databricks.labs.blueprint.installer import InstallState

logger = logging.getLogger(__name__)

_IGNORE_DIR_NAMES = {".git", ".venv", ".databricks", ".mypy_cache", ".github", ".idea", ".coverage", "htmlcov"}


class Wheels(AbstractContextManager):
    """Wheel builder"""

    __version: str | None = None

    def __init__(
        self,
        ws: WorkspaceClient,
        install_state: InstallState,
        *,
        github_org: str = "databrickslabs",
        verbose: bool = False,
        version_file_name: str = "__about__.py",
        project_root_finder: Callable[[], Path] | None = None,
    ):
        if not project_root_finder:
            project_root_finder = find_project_root
        self._ws = ws
        self._install_state = install_state
        self._github_org = github_org
        self._verbose = verbose
        self._version_file_name = version_file_name
        self._project_root_finder = project_root_finder

    def is_git_checkout(self) -> bool:
        project_root = self._project_root_finder()
        git_config = project_root / ".git" / "config"
        return git_config.exists()

    def is_unreleased_version(self) -> bool:
        return "+" in self.version()

    def released_version(self) -> str:
        project_root = self._project_root_finder()
        version_file = self._find_version_file(project_root, [self._version_file_name])
        return self._read_version(version_file)

    def version(self):
        """Returns current version of the project"""
        if hasattr(self, "__version"):
            return self.__version
        if not self.is_git_checkout():
            # normal install, downloaded releases won't have the .git folder
            self.__version = self.released_version()
            return self.__version
        try:
            self.__version = self._pep0440_version_from_git()
            return self.__version
        except subprocess.CalledProcessError as err:
            logger.error(
                "Cannot determine unreleased version. This can be fixed by adding "
                " `git fetch --prune --unshallow` to your CI configuration.",
                exc_info=err,
            )
            self.__version = self.released_version()
            return self.__version

    @staticmethod
    def _pep0440_version_from_git():
        out = subprocess.run(["git", "describe", "--tags"], stdout=subprocess.PIPE, check=True)  # noqa S607
        git_detached_version = out.stdout.decode("utf8")
        dv = SemVer.parse(git_detached_version)
        datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # new commits on main branch since the last tag
        new_commits = dv.pre_release.split("-")[0] if dv.pre_release else None
        # show that it's a version different from the released one in stats
        bump_patch = dv.patch + 1
        # create something that is both https://semver.org and https://peps.python.org/pep-0440/
        semver_and_pep0440 = f"{dv.major}.{dv.minor}.{bump_patch}+{new_commits}{datestamp}"
        # validate the semver
        SemVer.parse(semver_and_pep0440)
        return semver_and_pep0440

    def upload_to_dbfs(self) -> str:
        with self._local_wheel.open("rb") as f:
            self._ws.dbfs.mkdirs(self._remote_dir_name)
            logger.info(f"Uploading wheel to dbfs:{self._remote_wheel}")
            self._ws.dbfs.upload(self._remote_wheel, f, overwrite=True)
        return self._remote_wheel

    def upload_to_wsfs(self) -> str:
        with self._local_wheel.open("rb") as f:
            self._ws.workspace.mkdirs(self._remote_dir_name)
            logger.info(f"Uploading wheel to /Workspace{self._remote_wheel}")
            self._ws.workspace.upload(self._remote_wheel, f, overwrite=True, format=ImportFormat.AUTO)
        return self._remote_wheel

    def _find_version_file(self, root: Path, names: list[str]) -> Path:
        queue: list[Path] = [root]
        while queue:
            current = queue.pop(0)
            for file in current.iterdir():
                if file.name in names:
                    return file
                if not file.is_dir():
                    continue
                virtual_env_marker = file / "pyvenv.cfg"
                if virtual_env_marker.exists():
                    continue
                queue.append(file)
        raise NotImplementedError(f"cannot find {names} in {root}")

    @staticmethod
    def _read_version(version_file: Path) -> str:
        version_data: dict[str, str] = {}
        with version_file.open("r") as f:
            exec(f.read(), version_data)
        if "__version__" not in version_data:
            raise SyntaxError("Cannot find __version__")
        return version_data["__version__"]

    def __enter__(self) -> "Wheels":
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._local_wheel = self._build_wheel(self._tmp_dir.name, verbose=self._verbose)
        self._remote_wheel = f"{self._install_state.install_folder()}/wheels/{self._local_wheel.name}"
        self._remote_dir_name = os.path.dirname(self._remote_wheel)
        return self

    def __exit__(self, __exc_type, __exc_value, __traceback):
        self._tmp_dir.cleanup()

    def _build_wheel(self, tmp_dir: str, *, verbose: bool = False):
        """Helper to build the wheel package

        :param tmp_dir: str:
        :param *:
        :param verbose: bool:  (Default value = False)

        """
        stdout = subprocess.STDOUT
        stderr = subprocess.STDOUT
        if not verbose:
            stdout = subprocess.DEVNULL
            stderr = subprocess.DEVNULL
        project_root = self._project_root_finder()
        if self.is_git_checkout() and self.is_unreleased_version():
            # working copy becomes project root for building a wheel
            project_root = self._copy_root_to(tmp_dir)
            # and override the version file
            self._override_version_to_unreleased(project_root)
        logger.debug(f"Building wheel for {project_root} in {tmp_dir}")
        subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", tmp_dir, project_root.as_posix()],
            check=True,
            stdout=stdout,
            stderr=stderr,
        )
        # get wheel name as first file in the temp directory
        return next(Path(tmp_dir).glob("*.whl"))

    def _override_version_to_unreleased(self, tmp_dir_path: Path):
        version_file = self._find_version_file(tmp_dir_path, [self._version_file_name])
        with version_file.open("w") as f:
            f.write(f'__version__ = "{self.version()}"')

    def _copy_root_to(self, tmp_dir: str | Path):
        project_root = self._project_root_finder()
        tmp_dir_path = Path(tmp_dir) / "working-copy"
        # copy everything to a temporary directory
        shutil.copytree(project_root, tmp_dir_path, ignore=self._copy_ignore)
        return tmp_dir_path

    @staticmethod
    def _copy_ignore(_, names: list[str]):
        # callable(src, names) -> ignored_names
        ignored_names = []
        for name in names:
            if name not in _IGNORE_DIR_NAMES:
                continue
            ignored_names.append(name)
        return ignored_names
