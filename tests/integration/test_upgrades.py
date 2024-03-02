from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.wheels import ProductInfo


def test_upgrades_works(ws, make_random):
    simple_upgrades = Path(__file__).parent / "fixtures/simple_upgrades/__init__.py"
    product_info = ProductInfo(simple_upgrades.as_posix())
    # Installation.
    # def test_detect_global(ws, make_random):
    #     product = make_random(4)
    #     Installation(ws, product, install_folder=f"/Applications/{product}").upload("some", b"...")
