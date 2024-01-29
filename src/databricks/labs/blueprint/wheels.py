import datetime
import logging
import shutil
import subprocess
import sys
import tempfile
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.mixins.compute import SemVer

from databricks.labs.blueprint.entrypoint import find_project_root
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState

logger = logging.getLogger(__name__)

IGNORE_DIR_NAMES = {
    ".git",
    ".venv",
    ".databricks",
    ".mypy_cache",
    ".github",
    ".idea",
    ".coverage",
    "htmlcov",
    "__pycache__",
    "tests",
}


class ProductInfo:
    def __init__(
        self,
        __file: str,
        *,
        version_file_name: str = "__about__.py",
        github_org: str = "databrickslabs",
    ):
        self._project_root = find_project_root(__file)
        self._version_file_name = version_file_name
        self._github_org = github_org

    def project_root(self):
        # TODO: introduce the "in wheel detection", using the __about__.py as marker
        return self._project_root

    def version(self):
        """Returns current version of the project"""
        if hasattr(self, "__version"):
            return self.__version  # pylint: disable=access-member-before-definition
        if not self.is_git_checkout():
            # normal install, downloaded releases won't have the .git folder
            self.__version = self.released_version()
            return self.__version
        self.__version = self.unreleased_version()
        return self.__version

    def product_name(self) -> str:
        version_file = self.version_file_in(self._project_root)
        version_file_folder = version_file.parent
        return version_file_folder.name.replace("_", "-")

    def released_version(self) -> str:
        version_file = self.version_file_in(self._project_root)
        return self._read_version(version_file)

    def is_git_checkout(self) -> bool:
        git_config = self._project_root / ".git" / "config"
        return git_config.exists()

    def is_unreleased_version(self) -> bool:
        return "+" in self.version()

    def unreleased_version(self) -> str:
        try:
            out = subprocess.run(["git", "describe", "--tags"], stdout=subprocess.PIPE, check=True)  # noqa S607
            git_detached_version = out.stdout.decode("utf8")
            return self._semver_and_pep440(git_detached_version)
        except subprocess.CalledProcessError as err:
            logger.warning(
                "Cannot determine unreleased version. This can be fixed by adding "
                " `git fetch --prune --unshallow` to your CI configuration.",
                exc_info=err,
            )
            return self.released_version()

    @staticmethod
    def _semver_and_pep440(git_detached_version: str) -> str:
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

    def version_file_in(self, root: Path) -> Path:
        names = [self._version_file_name]
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
            exec(f.read(), version_data)  # pylint: disable=exec-used
        if "__version__" not in version_data:
            raise SyntaxError("Cannot find __version__")
        return version_data["__version__"]


@dataclass
class Version:
    version: str
    wheel: str


class WheelsV2(AbstractContextManager):
    """Wheel builder"""

    __version: str | None = None

    def __init__(self, installation: Installation, product_info: ProductInfo, *, verbose: bool = False):
        self._installation = installation
        self._product_info = product_info
        self._verbose = verbose

    def upload_to_dbfs(self) -> str:
        with self._local_wheel.open("rb") as f:
            return self._installation.upload_dbfs(f"wheels/{self._local_wheel.name}", f.read())

    def upload_to_wsfs(self) -> str:
        with self._local_wheel.open("rb") as f:
            remote_wheel = self._installation.upload(f"wheels/{self._local_wheel.name}", f.read())
            self._installation.save(Version(version=self._product_info.version(), wheel=remote_wheel))
            return remote_wheel

    def __enter__(self) -> "WheelsV2":
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._local_wheel = self._build_wheel(self._tmp_dir.name, verbose=self._verbose)
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
        project_root = self._product_info.project_root()
        if self._product_info.is_git_checkout() and self._product_info.is_unreleased_version():
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
        version_file = self._product_info.version_file_in(tmp_dir_path)
        with version_file.open("w") as f:
            f.write(f'__version__ = "{self._product_info.version()}"')

    def _copy_root_to(self, tmp_dir: str | Path):
        project_root = self._product_info.project_root()
        tmp_dir_path = Path(tmp_dir) / "working-copy"

        # copy everything to a temporary directory
        def copy_ignore(_, names: list[str]):
            # callable(src, names) -> ignored_names
            ignored_names = []
            for name in names:
                if name not in IGNORE_DIR_NAMES:
                    continue
                ignored_names.append(name)
            return ignored_names

        shutil.copytree(project_root, tmp_dir_path, ignore=copy_ignore)
        return tmp_dir_path


class Wheels(WheelsV2):
    """Wheel builder"""

    def __init__(
        self, ws: WorkspaceClient, install_state: InstallState, product_info: ProductInfo, *, verbose: bool = False
    ):
        installation = Installation(ws, product_info.product_name(), install_folder=install_state.install_folder())
        super().__init__(installation, product_info, verbose=verbose)
