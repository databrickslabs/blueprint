from dataclasses import dataclass
from enum import Enum
from typing import Any

import pytest

from databricks.labs.blueprint.installation import Installation, MockInstallation

class TypeSupport(Enum):
    STRICT = "STRICT"
    RAW_TYPES = "RAW"
    WEAK_TYPES = "WEAK"

def set_type_support(type_support: TypeSupport):
    if type_support == TypeSupport.STRICT:
        Installation.allow_raw_types = False
        Installation.allow_weak_types = False
    elif type_support == TypeSupport.RAW_TYPES:
        Installation.allow_raw_types = True
        Installation.allow_weak_types = False
    if type_support == TypeSupport.WEAK_TYPES:
        Installation.allow_raw_types = True
        Installation.allow_weak_types = True

@pytest.mark.parametrize("type_support", [s for s in TypeSupport])
def test_weak_typing_with_list(type_support) -> None:
    set_type_support(type_support)
    # this example corresponds to a frequent Python coding pattern
    # where users don't specify the item type of a list

    @dataclass
    class SampleClass:
        field: list

    installation = MockInstallation()
    saved = SampleClass(field=["a", 1, True])
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved


@pytest.mark.parametrize("type_support", [s for s in TypeSupport])
def test_weak_typing_with_dict(type_support) -> None:
    set_type_support(type_support)
    # this example corresponds to a frequent Python coding pattern
    # where users don't specify the key and item types of a dict

    @dataclass
    class SampleClass:
        field: dict

    installation = MockInstallation()
    saved = SampleClass(field={"x": "a", "y": 1, "z": True})
    installation.save(saved, filename="backups/SampleClass.json")
    loaded = installation.load(SampleClass, filename="backups/SampleClass.json")
    assert loaded == saved

@pytest.mark.parametrize("type_support", [s for s in TypeSupport])
def test_progressive_typing_with_list(type_support) -> None:
    set_type_support(type_support)

    # this example corresponds to a frequent Python coding pattern
    # where users only specify the item type of a list once they need it

    @dataclass
    class SampleClassV1:
        field: list

    @dataclass
    class SampleClassV2:
        field: list[str]

    installation = MockInstallation()
    saved = SampleClassV1(field=["a", "b", "c"])
    installation.save(saved, filename="backups/SampleClass.json")
    # problem: can't directly use untyped item values
    # loaded_v1 = installation.load(SampleClassV1, filename="backups/SampleClass.json")
    # stuff = loaded_v1[0][1:2]
    # so they've stored weakly typed data, and they need to read it as typed data
    loaded = installation.load(SampleClassV2, filename="backups/SampleClass.json")
    assert loaded == SampleClassV2(field=saved.field)


@pytest.mark.parametrize("type_support", [s for s in TypeSupport])
def test_progressive_typing_with_dict(type_support) -> None:
    set_type_support(type_support)

    # this example corresponds to a frequent Python coding pattern
    # where users only specify the item type of a dict once they need it

    @dataclass
    class SampleClassV1:
        field: dict

    @dataclass
    class SampleClassV2:
        field: dict[str, str]

    installation = MockInstallation()
    saved = SampleClassV1(field={"x": "abc", "y": "def", "z": "ghi"})
    installation.save(saved, filename="backups/SampleClass.json")
    # problem: can't directly use untyped item values
    # loaded_v1 = installation.load(SampleClassV1, filename="backups/SampleClass.json")
    # stuff = loaded_v1["x"][1:2]
    # so they've stored weakly typed data, and they need to read it as typed data
    loaded = installation.load(SampleClassV2, filename="backups/SampleClass.json")
    assert loaded == SampleClassV2(field=saved.field)

@pytest.mark.parametrize("type_support", [s for s in TypeSupport])
def test_type_migration(type_support) -> None:
    set_type_support(type_support)

    # this example corresponds to a frequent Python coding scenario
    # where users change their mind about a type

    @dataclass
    class SampleClassV1:
        field: list[str]

    @dataclass
    class SampleClassV2:
        field: list[int | None]

    installation = MockInstallation()
    saved = SampleClassV1(field=["1", "2", ""])
    installation.save(saved, filename="backups/SampleClass.json")
    # problem: can't directly convert an item value
    # loaded_v1 = installation.load(SampleClassV2, filename="backups/SampleClass.json")
    # so they've stored strings, and they need to read ints
    converted = SampleClassV2(field=[(int(val) if val else None) for val in saved.field])
    loaded = installation.load(SampleClassV2, filename="backups/SampleClass.json")
    assert loaded == converted

@pytest.mark.parametrize("type_support", [s for s in TypeSupport])
def test_lost_code_with_list(type_support) -> None:
    set_type_support(type_support)
    # this example corresponds to a scenario where data was stored
    # using code that is no longer available

    @dataclass
    class LostSampleClass:
        field: list[str]

    # we don't know the type of 'field'
    # so we'll use code to restore the data
    @dataclass
    class RecoverySampleClass:
        field: object

    installation = MockInstallation()
    saved = LostSampleClass(field=["a", "b", "c"])
    installation.save(saved, filename="backups/SampleClass.json")
    # problem: we don't know how SampleClass.json was stored
    # so we're loading the data as weakly typed
    loaded = installation.load(RecoverySampleClass, filename="backups/SampleClass.json")
    assert loaded.field == saved.field


@pytest.mark.parametrize("type_support", [s for s in TypeSupport])
def test_lost_code_with_any(type_support) -> None:
    set_type_support(type_support)
    # this example corresponds to a scenario where data was stored
    # using code that is no longer available

    @dataclass
    class LostSampleClass:
        field: dict[str, str | int | None]

    # we don't know the type of 'field'
    # so we'll use code to restore the data
    @dataclass
    class RecoverySampleClass:
        field: Any

    installation = MockInstallation()
    saved = LostSampleClass(field={"a": "b", "b": 2, "c": None})
    installation.save(saved, filename="backups/SampleClass.json")
    # problem: we don't know how SampleClass.json was stored
    # so we're loading the data as weakly typed
    loaded = installation.load(RecoverySampleClass, filename="backups/SampleClass.json")
    assert loaded.field == saved.field
