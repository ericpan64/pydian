from typing import Any

import pytest

import pydian.partials as p
from pydian import get
from pydian.dicts import drop_keys


def test_get(simple_data: dict[str, Any]) -> None:
    source = simple_data

    assert get(source, "data") == source.get("data")
    assert get(source, "data.patient.id") == source["data"]["patient"]["id"]
    assert get(source, "data.patient.active") == source["data"]["patient"]["active"]
    assert (
        get(source, "data.patient.id", apply=lambda s: s + "_modified")
        == source["data"]["patient"]["id"] + "_modified"
    )


def test_get_index(simple_data: dict[str, Any]) -> None:
    source = simple_data

    # Indexing
    assert get(source, "list_data[0].patient") == source["list_data"][0]["patient"]
    assert get(source, "list_data[1].patient") == source["list_data"][1]["patient"]
    assert get(source, "list_data[5000].patient") is None
    assert get(source, "list_data[-1].patient") == source["list_data"][-1]["patient"]
    # Slicing
    assert get(source, "list_data[1:3]") == source["list_data"][1:3]
    assert get(source, "list_data[1:]") == source["list_data"][1:]
    assert get(source, "list_data[:2]") == source["list_data"][:2]
    assert get(source, "list_data[:]") == source["list_data"][:]


def test_get_from_list(list_data: list[Any]) -> None:
    source = list_data

    assert get(source, "[*].patient") == [p["patient"] for p in source]
    assert get(source, "[*].patient.id") == [p["patient"]["id"] for p in source]
    assert get(source, "[0].patient.id") == source[0]["patient"]["id"]
    assert get(source, "[-1].patient.id") == source[-1]["patient"]["id"]
    assert get(source, "[0:2].patient.id") == [p["patient"]["id"] for p in source[0:2]]
    assert get(source, "[-2:].patient.id") == [p["patient"]["id"] for p in source[-2:]]


def test_nested_get(nested_data: dict[str, Any]) -> None:
    source = nested_data

    assert get(source, "data[*].patient.active") == [True, False, True, True]
    assert get(source, "data[*].patient.id") == ["abc123", "def456", "ghi789", "jkl101112"]
    assert get(source, "data[*].patient.ints") == [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    assert get(source, "data[*].patient.ints", flatten=True) == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert get(source, "data[*].patient.dicts[*].num") == [[1, 2], [3, 4], [5, 6], [7]]
    assert get(source, "data[*].patient.dicts[*].num", flatten=True) == [1, 2, 3, 4, 5, 6, 7]
    assert get(source, "missing.key") is None
    assert get(source, "missing[*].key") is None
    assert get(source, "missing[*].key[*].here") is None
    assert get(source, "data[8888].patient") is None


def test_drop_keys(nested_data: dict[str, Any]) -> None:
    source = nested_data

    keys_to_drop = {
        "data[0].patient",
        "data[2].patient.id",
        "data[3].patient.active",
    }

    res = drop_keys(source, keys_to_drop)
    for k in keys_to_drop:
        assert get(res, k) is None


def test_get_apply(simple_data: dict[str, Any]) -> None:
    source = simple_data

    OLD_STR, NEW_STR = "456", "FourFiveSix"
    single_apply = str.upper
    chained_apply = [str.upper, p.do(str.replace, OLD_STR, NEW_STR)]
    failed_chain_apply = [str.upper, lambda _: None, p.do(str.replace, OLD_STR, NEW_STR)]

    assert get(source, "data.patient.id", apply=single_apply) == str.upper(
        source["data"]["patient"]["id"]
    )
    assert get(source, "list_data[0].patient.id", apply=chained_apply) == (
        str.upper(source["list_data"][0]["patient"]["id"])
    ).replace(OLD_STR, NEW_STR)
    assert get(source, "list_data[0].patient.id", apply=failed_chain_apply) is None
    assert get(source, "data.notFoundKey", apply=chained_apply) is None


def test_get_only_if(simple_data: dict[str, Any]) -> None:
    source = simple_data

    KEY = "data.patient.id"
    passes_check = get(source, KEY, only_if=lambda s: str.startswith(s, "abc"), apply=str.upper)
    fails_check = get(source, KEY, only_if=lambda s: str.startswith(s, "000"), apply=str.upper)

    assert passes_check == source["data"]["patient"]["id"].upper()
    assert fails_check is None


def test_get_single_key_tuple(simple_data: dict[str, Any]) -> None:
    source = simple_data

    assert get(source, "data.patient.[id,active]") == [
        source["data"]["patient"]["id"],
        source["data"]["patient"]["active"],
    ]

    # Handle tuple case
    assert get(source, "data.patient.(id,active)") == (
        source["data"]["patient"]["id"],
        source["data"]["patient"]["active"],
    )

    # Allow whitespace within brackets
    assert get(source, "data.patient.[id, active]") == get(source, "data.patient.[id,active]")
    assert get(source, "data.patient.[id,active,missingKey]") == [
        source["data"]["patient"]["id"],
        source["data"]["patient"]["active"],
        None,
    ]

    # Expect list unwrapping to still work
    assert get(source, "list_data[*].patient.[id, active]") == [
        [p["patient"]["id"], p["patient"]["active"]] for p in source["list_data"]
    ]
    assert get(source, "list_data[*].patient.[id, active, missingKey]") == [
        [p["patient"]["id"], p["patient"]["active"], None] for p in source["list_data"]
    ]

    # Test default (expect at each tuple item on a failed get)
    STR_DEFAULT = "Missing!"
    assert get(source, "data.patient.[id, active, missingKey]", default=STR_DEFAULT) == [
        source["data"]["patient"]["id"],
        source["data"]["patient"]["active"],
        STR_DEFAULT,
    ]

    # Test apply
    assert (
        get(source, "data.patient.[id, active, missingKey]", apply=p.index(1))
        == source["data"]["patient"]["active"]
    )
    assert get(source, "data.patient.[id, active, missingKey]", apply=p.keep(2)) == [
        source["data"]["patient"]["id"],
        source["data"]["patient"]["active"],
    ]

    # Test only_if filtering
    assert get(source, "data.patient.[id, active, missingKey]", only_if=lambda _: False) is None


def test_get_nested_key_tuple(nested_data: dict[str, Any]) -> None:
    source = nested_data

    # Single item example
    single_item_example = source["data"][0]["patient"]["dicts"][0]
    assert get(source, "data[0].patient.dicts[0].[num, text]") == [
        single_item_example["num"],
        single_item_example["text"],
    ]
    assert get(source, "data[0].patient.dicts[0].[num,inner.msg]") == [
        single_item_example["num"],
        single_item_example["inner"]["msg"],
    ]

    # Multi-item example
    assert get(source, "data[*].patient.dict.[char, inner.msg]") == [
        [d["patient"]["dict"]["char"], d["patient"]["dict"]["inner"]["msg"]] for d in source["data"]
    ]

    # Multi-item on multi-[*] example
    assert get(source, "data[*].patient.dicts[*].[num, inner.msg]") == [
        [[obj["num"], obj["inner"]["msg"]] for obj in d["patient"]["dicts"]] for d in source["data"]
    ]


def test_get_strict(nested_data: dict[str, Any]) -> None:
    source = nested_data

    # Simple key example
    MISSING_KEY = "some.key.nope.notthere"
    with pytest.raises(ValueError) as exc_info:
        get(source, MISSING_KEY, strict=True)
    assert get(source, MISSING_KEY) == None
