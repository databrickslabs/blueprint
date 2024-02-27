from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.upgrades import Upgrades


def test_upgrades_works():
    installation = MockInstallation({"version.json": {"version": "0.0.1"}})
    upgrades = Upgrades(installation)
    # upgrades.
