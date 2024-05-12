from databricks.labs.blueprint.wheels import ProductInfo, Version, WheelsV2


def test_upload_wsfs(ws, new_installation):
    product_info = ProductInfo.from_class(WheelsV2)
    with WheelsV2(new_installation, product_info) as whl:
        remote_wheel = whl.upload_to_wsfs()
        ws.workspace.get_status(remote_wheel)
    version = new_installation.load(Version)
    assert version.wheel == remote_wheel
    # unreleased versions contain milliseconds
    assert version.version[:-3] == product_info.version()[:-3]


def test_upload_dbfs(ws, new_installation):
    product_info = ProductInfo.from_class(WheelsV2)
    with WheelsV2(new_installation, product_info) as whl:
        remote_wheel = whl.upload_to_dbfs()
        ws.dbfs.get_status(remote_wheel)


def test_upload_upstreams(ws, new_installation):
    product_info = ProductInfo.from_class(WheelsV2)
    with WheelsV2(new_installation, product_info) as whl:
        whl.upload_wheel_dependencies(["databricks"])

        installation_files = new_installation.files()
        # only Databricks SDK has to be uploaded
        assert len(installation_files) == 1

        whl.upload_to_wsfs()
        installation_files = new_installation.files()
        # SDK, Blueprint and version.json metadata
        assert len(installation_files) == 3
