from pathlib import Path
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.upgrades import Upgrades
from databricks.labs.blueprint.wheels import ProductInfo


def test_upgrades_work():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"

    with_upgrades = Path(__file__).parent / "fixtures/with_upgrades/__init__.py"
    product_info = ProductInfo(with_upgrades.as_posix())
    installation = MockInstallation({"version.json": {"version": "0.1.0", "wheel": "...", "date": "..."}})
    upgrades = Upgrades(product_info, installation)

    upgrades.apply(ws)

    ws.jobs.list.assert_not_called()
    ws.clusters.list.assert_not_called()
    ws.workspace.list.assert_called_with("/Some/v0.1.3")
    ws.workspace.delete.assert_called_with("/Some/v0.2.0")

    installation.assert_file_written(
        "applied-upgrades.json", {"upgrades": ["v0.1.3_other.py", "v0.2.0_another_breaking_change.py"], "version": 1}
    )


def test_upgrades_already_applied():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me().user_name = "foo"

    with_upgrades = Path(__file__).parent / "fixtures/with_upgrades/__init__.py"
    product_info = ProductInfo(with_upgrades.as_posix())
    installation = MockInstallation(
        {
            "version.json": {"version": "0.1.0", "wheel": "...", "date": "..."},
            "applied-upgrades.json": {"upgrades": ["v0.1.3_other.py"]},
        }
    )
    upgrades = Upgrades(product_info, installation)

    upgrades.apply(ws)

    ws.workspace.list.assert_not_called()
    ws.workspace.delete.assert_called_with("/Some/v0.2.0")

    installation.assert_file_written(
        "applied-upgrades.json", {"upgrades": ["v0.1.3_other.py", "v0.2.0_another_breaking_change.py"], "version": 1}
    )


def test_no_upgrades_folder():
    ws = create_autospec(WorkspaceClient)
    no_upgrades = Path(__file__).parent / "fixtures/no_upgrades/__init__.py"
    product_info = ProductInfo(no_upgrades.as_posix())
    installation = MockInstallation(
        {
            "version.json": {"version": "0.1.0", "wheel": "...", "date": "..."},
        }
    )
    upgrades = Upgrades(product_info, installation)

    upgrades.apply(ws)
