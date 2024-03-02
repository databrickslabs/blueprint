import os.path
from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.upgrades import AppliedUpgrades, Upgrades
from databricks.labs.blueprint.wheels import ProductInfo, Version


def test_upgrades_works(ws, make_random):
    simple_upgrades = Path(__file__).parent / "fixtures/simple_upgrades/__init__.py"
    product_info = ProductInfo(simple_upgrades.as_posix())
    installation = Installation.assume_user_home(ws, product_info.product_name())
    installation.save(Version("0.1.0", "...", "..."))

    upgrades = Upgrades(product_info, installation)
    upgrades.apply(ws)

    side_effect = {os.path.basename(_.path) for _ in installation.files()}
    assert side_effect == {"version.json", "applied-upgrades.json", "v0.1.3", "v0.2.0"}

    applied = installation.load(AppliedUpgrades)
    assert applied.upgrades == ["v0.1.3_other.py", "v0.2.0_another_breaking_change.py"]
