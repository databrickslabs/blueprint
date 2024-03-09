"""Automated rollout of application upgrades deployed in a Databricks workspace."""

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
    """Automated rollout of application upgrades deployed in a Databricks workspace.

    As time goes by, your applications evolve as well, requiring the addition of new columns to database schemas,
    changes of the database state, or some migrations of configured workflows. This utility allows you to do seamless
    upgrades from version X to version Z through version Y. Idiomatic usage in your deployment automation is as follows:

    >>> from ... import Config
    >>> from databricks.sdk import WorkspaceClient
    >>> from databricks.labs.blueprint.upgrades import Upgrades
    >>> from databricks.labs.blueprint.wheels import ProductInfo
    >>>
    >>> product_info = ProductInfo.from_class(Config)
    >>> ws = WorkspaceClient(product=product_info.product_name(), product_version=product_info.version())
    >>> installation = product_info.current_installation(ws)
    >>> config = installation.load(Config)
    >>> upgrades = Upgrades(product_info, installation)
    >>> upgrades.apply(ws)

    The upgrade process loads the version of the product that is about to be installed from `__about__.py` file that
    declares the `__version__` variable. This version is compares with the version currently installed on
    the Databricks Workspace by loading it from the `version.json` file in the installation folder. This file is kept
    up-to-date automatically if you use the databricks.labs.blueprint.wheels.WheelsV2.

    If those versions are different, the process looks for the `upgrades` folder next to `__about__.py` file and
    computes a difference for the upgrades in need to be rolled out. Every upgrade script in that directory has to
    start with a valid SemVer identifier, followed by the alphanumeric description of the change,
    like `v0.0.1_add_service.py`. Each script has to expose a function that takes `Installation` and
    `WorkspaceClient` arguments to perform the relevant upgrades. Here's the example:

    >>> import logging, dataclasses
    >>> from databricks.sdk import WorkspaceClient
    >>> from databricks.labs.blueprint.installation import Installation
    >>> from ... import Config
    >>> upgrade_logger = logging.getLogger(__name__)
    >>>
    >>> def upgrade(installation: Installation, ws: WorkspaceClient):
    >>>     upgrade_logger.info(f"creating new automated service user for the installation")
    >>>     config = installation.load(Config)
    >>>     service_principal = ws.service_principals.create(display_name='blueprint-service')
    >>>     new_config = dataclasses.replace(config, application_id=service_principal.application_id)
    >>>     installation.save(new_config)

    To prevent the same upgrade script from being applies twice, we use `applied-upgrades.json` file in
    the installation directory.
    """

    def __init__(self, product_info: ProductInfo, installation: Installation):
        self._product_info = product_info
        self._installation = installation

    def apply(self, ws: WorkspaceClient):
        """Applies application state upgrades for the given installation."""
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
        """Load and apply the upgrade script."""
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
        """Load the installed version of the product."""
        return self._installation.load(Version).as_semver()

    def _diff(self, upgrades_folder: Path):
        """Yield the upgrade scripts that need to be applied."""
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
        """Parse the version from the upgrade script name."""
        split = name.split("_")
        if len(split) < 2:
            raise ValueError(f"invalid spec: {name}")
        return SemVer.parse(split[0])
