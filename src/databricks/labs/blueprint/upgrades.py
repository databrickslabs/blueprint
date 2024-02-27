import importlib.util
import logging
from dataclasses import dataclass, field
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.mixins.compute import SemVer

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.wheels import ProductInfo, Version

logger = logging.getLogger(__name__)


@dataclass
class AppliedUpgrades:
    __version__ = 1
    upgrades: list[str] = field(default_factory=list)


class Upgrades:
    def __init__(self, product_info: ProductInfo, installation: Installation):
        self._product_info = product_info
        self._installation = installation

    def apply(self, ws: WorkspaceClient):
        upgrades_folder = self._product_info.version_file().parent / "upgrades"
        if not upgrades_folder.exists():
            logger.warning(f"No upgrades folder: {upgrades_folder}")
            return
        applied = self._installation.load_or_default(AppliedUpgrades)
        for script in self._diff(upgrades_folder):
            if script.name in applied.upgrades:
                logger.info(f"Already applied: {script.name}")
                continue
            self._apply_python_script(script, ws)
            applied.upgrades.append(script.name)
            self._installation.save(applied)

    def _apply_python_script(self, script: Path, ws: WorkspaceClient):
        name = "_".join(script.name.removesuffix(".py").split("_")[1:])
        spec = importlib.util.spec_from_file_location(name, script.as_posix())
        if not spec:
            logger.warning(f"Cannot load: {script.name}")
            return
        change = importlib.util.module_from_spec(spec)
        if not spec.loader:
            logger.warning(f"No loader: {script.name}")
            return
        spec.loader.exec_module(change)
        if not hasattr(change, "upgrade"):
            logger.warning(f"No upgrade(installation, ws) callback: {script.name}")
            return
        logger.info(f"Applying {script.name}...")
        change.upgrade(self._installation, ws)

    def _installed(self) -> SemVer:
        return self._installation.load(Version).as_semver()

    def _diff(self, upgrades_folder: Path):
        current = self._product_info.as_semver()
        installed_version = self._installed()
        for file in upgrades_folder.glob("v*.py"):
            try:
                semver = self._parse_version(file.name)
            except ValueError:
                logger.warning(f"not an upgrade script: {file.name}")
                continue
            if semver < installed_version:
                continue
            if semver > current:
                logger.warning(f"future version: {file.name}")
                continue
            yield file

    @staticmethod
    def _parse_version(name: str) -> SemVer:
        split = name.split("_")
        if len(split) < 1:
            raise ValueError(f"invalid spec: {name}")
        return SemVer.parse(split[0])
