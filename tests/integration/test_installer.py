from databricks.labs.blueprint.installer import InstallState


def test_no_state(new_installation):
    state = InstallState.from_installation(new_installation)
    assert {} == state.jobs
