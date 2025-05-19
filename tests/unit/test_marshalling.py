from dataclasses import dataclass

import pytest
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation, MockInstallation


@pytest.mark.parametrize("allow_weak_types", [ True, False ])
def test_weak_typing_with_list(allow_weak_types) -> None:

    # this example corresponds to a frequent Python coding pattern
    # where users don't specify the item type of a list

    @dataclass
    class SampleClass:
        field: list

    Installation.allow_weak_types = allow_weak_types
    installation = MockInstallation()
    saved = SampleClass(field=["a", 1, True])
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved


@pytest.mark.parametrize("allow_weak_types", [True, False])
def test_weak_typing_with_dict(allow_weak_types) -> None:
    # this example corresponds to a frequent Python coding pattern
    # where users don't specify the key and item types of a dict

    @dataclass
    class SampleClass:
        field: dict

    Installation.allow_weak_types = allow_weak_types
    installation = MockInstallation()
    saved = SampleClass(field={"x": "a", "y": 1, "z": True})
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved
