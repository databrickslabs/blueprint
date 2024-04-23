"""Product info and wheel builder."""

import inspect
import logging
import random
import shutil
import string
import subprocess
import sys
import tempfile
import warnings
from collections.abc import Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
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


class SingleSourceVersionError(NotImplementedError):
    pass


class ProductInfo:
    _version_file_names = ["__about__.py", "__version__.py", "version.py"]

    def __init__(self, __file: str, *, github_org: str = "databrickslabs", product_name: str | None = None):
        self._version_file = self._infer_version_file(Path(__file), self._version_file_names)
        self._product_name = product_name
        self._github_org = github_org

    @classmethod
    def from_class(cls, klass: type) -> "ProductInfo":
        """Create a product info with a class used as a starting point to determine location of the version file."""
        return cls(inspect.getfile(klass))

    @classmethod
    def for_testing(cls, klass: type) -> "ProductInfo":
        """Create a product info for testing purposes with a random product name."""
        return cls(inspect.getfile(klass), product_name=cls._make_random(4))

    def checkout_root(self):
        """Returns the root of the project, where .git folder is located."""
        return find_project_root(self._version_file.as_posix())

    def version_file(self) -> Path:
        """Returns the path to a file, where __version__ variable is defined.

        The path to this package can be thought as a way to determine the other
        assets of your deployed wheel.

        See https://packaging.python.org/guides/single-sourcing-package-version/"""
        return self._version_file

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

    def as_semver(self) -> SemVer:
        """Returns the version as SemVer object."""
        return SemVer.parse(self.version())

    def product_name(self) -> str:
        """Returns the product name based on the version file folder name."""
        if self._product_name:
            return self._product_name
        version_file_folder = self._version_file.parent
        return version_file_folder.name.replace("_", "-")

    def released_version(self) -> str:
        """Returns the version from the version file."""
        return self._read_version(self._version_file)

    def is_git_checkout(self) -> bool:
        """Returns True if the project is a git checkout."""
        git_config = self.checkout_root() / ".git" / "config"
        return git_config.exists()

    def is_unreleased_version(self) -> bool:
        """Returns True if we are in the git checkout and the version is unreleased."""
        return "+" in self.version()

    def unreleased_version(self) -> str:
        """Returns the unreleased version based on the `git describe --tags` output."""
        try:
            out = subprocess.run(
                ["git", "describe", "--tags"], stdout=subprocess.PIPE, check=True, cwd=self.checkout_root()
            )  # noqa S607
            git_detached_version = out.stdout.decode("utf8")
            return self._semver_and_pep440(git_detached_version)
        except subprocess.CalledProcessError as err:
            logger.warning(
                "Cannot determine unreleased version. This can be fixed by adding "
                " `git fetch --prune --unshallow` to your CI configuration.",
                exc_info=err,
            )
            return self.released_version()

    def current_installation(self, ws: WorkspaceClient) -> Installation:
        """Returns the current installation of the product."""
        return Installation.current(ws, self.product_name())

    def wheels(self, ws: WorkspaceClient) -> "WheelsV2":
        """Returns the wheel builder."""
        return WheelsV2(self.current_installation(ws), self)

    @staticmethod
    def _make_random(k) -> str:
        """Generate a random string of fixed length"""
        # get a random meaningful word from the system dictionary if it exists
        system_wordlist = Path("/usr/share/dict/words")
        if system_wordlist.exists():
            with system_wordlist.open("r", encoding=sys.getdefaultencoding()) as f:
                at_lest_len = [_.lower() for _ in f.read().splitlines() if len(_) > k]
                first = random.choice(at_lest_len)
                second = random.choice(at_lest_len)
                return f"{first}-{second}"
        charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return "".join(random.choices(charset, k=int(k)))

    @staticmethod
    def _semver_and_pep440(git_detached_version: str) -> str:
        """Create a version that is both SemVer and PEP440 compliant."""
        detached_version = SemVer.parse(git_detached_version)
        datestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        # new commits on main branch since the last tag
        new_commits = detached_version.pre_release.split("-")[0] if detached_version.pre_release else None
        # show that it's a version different from the released one in stats
        bump_patch = detached_version.patch + 1
        # create something that is both https://semver.org and https://peps.python.org/pep-0440/
        semver_and_pep0440 = f"{detached_version.major}.{detached_version.minor}.{bump_patch}+{new_commits}{datestamp}"
        # validate the semver
        SemVer.parse(semver_and_pep0440)
        return semver_and_pep0440

    @classmethod
    def _infer_version_file(cls, start: Path, version_file_names: list[str]) -> Path:
        # be aware, that WheelsV2 overwrites this wheel file with unreleased version identifier,
        # if it's a git checkout, so that's why we cannot use __init__.py as version marker file,
        # at least for now.
        for version_file in cls._traverse_up(start, version_file_names):
            try:
                cls._read_version(version_file)
                return version_file
            except SyntaxError:
                continue
        candidates = " or ".join(version_file_names)
        raise SingleSourceVersionError(f"cannot find {candidates} with __version__ variable in the tree of {start}")

    @staticmethod
    def _traverse_up(start: Path, version_file_names: list[str]) -> Iterable[Path]:
        """Traverse up the directory tree and yield the version files."""
        prev_folder = start
        folder = start.parent
        while not folder.samefile(prev_folder):
            for name in version_file_names:
                candidate = folder / name
                if not candidate.exists():
                    continue
                yield candidate
            prev_folder = folder
            folder = folder.parent

    @staticmethod
    def _read_version(version_file: Path) -> str:
        """Read the version from the version file."""
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
    date: str

    def as_semver(self) -> SemVer:
        return SemVer.parse(self.version)


class WheelsV2(AbstractContextManager):
    """Wheel builder"""

    __version: str | None = None

    def __init__(self, installation: Installation, product_info: ProductInfo, *, verbose: bool = False):
        self._installation = installation
        self._product_info = product_info
        self._verbose = verbose

    def upload_to_dbfs(self) -> str:
        """Uploads the wheel to DBFS location of installation and returns the remote path."""
        with self._local_wheel.open("rb") as f:
            return self._installation.upload_dbfs(f"wheels/{self._local_wheel.name}", f)

    def upload_to_wsfs(self) -> str:
        """Uploads the wheel to WSFS location of installation and returns the remote path."""
        with self._local_wheel.open("rb") as f:
            remote_wheel = self._installation.upload(f"wheels/{self._local_wheel.name}", f.read())
            self._installation.save(Version(self._product_info.version(), remote_wheel, self._now_iso()))
            return remote_wheel

    @staticmethod
    def _now_iso():
        """Returns the current time in ISO format."""
        return datetime.now(timezone.utc).isoformat()

    def __enter__(self) -> "WheelsV2":
        """Builds the wheel and returns the instance. Use it as a context manager."""
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._local_wheel = self._build_wheel(self._tmp_dir.name, verbose=self._verbose)
        return self

    def __exit__(self, __exc_type, __exc_value, __traceback):
        """Cleans up the temporary directory. Use it as a context manager."""
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
        checkout_root = self._product_info.checkout_root()
        if self._product_info.is_git_checkout() and self._product_info.is_unreleased_version():
            # working copy becomes project root for building a wheel
            checkout_root = self._copy_root_to(tmp_dir)
            # and override the version file
            self._override_version_to_unreleased(checkout_root)
        logger.debug(f"Building wheel for {checkout_root} in {tmp_dir}")
        subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", tmp_dir, checkout_root.as_posix()],
            check=True,
            stdout=stdout,
            stderr=stderr,
        )
        # get wheel name as first file in the temp directory
        return next(Path(tmp_dir).glob("*.whl"))

    def _override_version_to_unreleased(self, tmp_dir_path: Path):
        """Overrides the version file to unreleased version."""
        checkout_root = self._product_info.checkout_root()
        relative_version_file = self._product_info.version_file().relative_to(checkout_root)
        version_file = tmp_dir_path / relative_version_file
        with version_file.open("w") as f:
            f.write(f'__version__ = "{self._product_info.version()}"')

    def _copy_root_to(self, tmp_dir: str | Path):
        """Copies the root to a temporary directory."""
        checkout_root = self._product_info.checkout_root()
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

        shutil.copytree(checkout_root, tmp_dir_path, ignore=copy_ignore)
        return tmp_dir_path


class Wheels(WheelsV2):
    """Wheel builder"""

    def __init__(
        self, ws: WorkspaceClient, install_state: InstallState, product_info: ProductInfo, *, verbose: bool = False
    ):
        warnings.warn("Wheels is deprecated, use WheelsV2 instead", DeprecationWarning)
        installation = Installation(ws, product_info.product_name(), install_folder=install_state.install_folder())
        super().__init__(installation, product_info, verbose=verbose)
