from databricks.sdk.mixins.compute import SemVer

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.wheels import Version


class Upgrades:
    def __init__(self, installation: Installation):
        self._installation = installation

    def apply(self):
        pass

    def _installed(self) -> SemVer:
        return self._installation.load(Version).as_semver()

    def _diff(self, names: list[str]):
        installed_version = self._installed()
        for name in names:
            semver = self._parse_version(name)
            if not semver < installed_version:
                continue
            yield name

    @staticmethod
    def _parse_version(name: str) -> SemVer:
        split = name.split("_")
        if len(split) < 1:
            raise ValueError(f"invalid spec: {name}")
        return SemVer.parse(split[0])
