import pytest
from pydian import Mapper, ROL, get


@pytest.fixture
def simple_data() -> dict:
    return {
        "data": {"patient": {"id": "abc123", "active": True}},
        "list_data": [
            {"patient": {"id": "def456", "active": True}},
            {"patient": {"id": "ghi789", "active": False}},
        ],
    }


@pytest.fixture
def nested_data() -> dict:
    return {
        "data": [
            {
                "patient": {
                    "id": "abc123",
                    "active": True,
                    "ints": [1, 2, 3],
                    "dicts": [{"num": 1}, {"num": 2}],
                }
            },
            {
                "patient": {
                    "id": "def456",
                    "active": False,
                    "ints": [4, 5, 6],
                    "dicts": [{"num": 3}, {"num": 4}],
                }
            },
            {
                "patient": {
                    "id": "ghi789",
                    "active": True,
                    "ints": [7, 8, 9],
                    "dicts": [{"num": 5}, {"num": 6}],
                }
            },
            {
                "patient": {
                    "id": "jkl101112",
                    "active": True,
                    # 'ints' is deliberately missing
                    "dicts": [{"num": 7}],
                }
            },
        ]
    }


def test_get(simple_data):
    source = simple_data
    mod_fn = lambda msg: msg["data"]["patient"]["id"] + "_modified"

    def mapping(m: dict) -> dict:
        return {
            "CASE_constant": 123,
            "CASE_single": get(m, "data"),
            "CASE_nested": get(m, "data.patient.id"),
            "CASE_nested_as_list": [get(m, "data.patient.active")],
            "CASE_modded": mod_fn(m),
            "CASE_index_list": {
                "first": get(m, "list_data[0].patient"),
                "second": get(m, "list_data[1].patient"),
                "out_of_bounds": get(m, "list_data[2].patient"),
            },
        }

    mapper = Mapper(mapping, remove_empty=True)
    res = mapper(source)
    assert res == {
        "CASE_constant": 123,
        "CASE_single": source.get("data"),
        "CASE_nested": source["data"]["patient"]["id"],
        "CASE_nested_as_list": [source["data"]["patient"]["active"]],
        "CASE_modded": mod_fn(source),
        "CASE_index_list": {
            "first": source["list_data"][0]["patient"],
            "second": source["list_data"][1]["patient"],
        },
    }


def test_nested_get(nested_data):
    source = nested_data

    def mapping(m: dict):
        return {
            "CASE_constant": 123,
            "CASE_unwrap_active": get(m, "data[*].patient.active"),
            "CASE_unwrap_id": get(m, "data[*].patient.id"),
            "CASE_unwrap_list": get(m, "data[*].patient.ints"),
            "CASE_unwrap_list_twice": get(m, "data[*].patient.ints[*]"),
            "CASE_unwrap_list_dict": get(m, "data[*].patient.dicts[*].num"),
            "CASE_unwrap_list_dict_twice": get(m, "data[*].patient.dicts[*].num[*]"),
        }

    mapper = Mapper(mapping, remove_empty=True)
    res = mapper(source)
    assert res == {
        "CASE_constant": 123,
        "CASE_unwrap_active": [True, False, True, True],
        "CASE_unwrap_id": ["abc123", "def456", "ghi789", "jkl101112"],
        "CASE_unwrap_list": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        "CASE_unwrap_list_twice": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        "CASE_unwrap_list_dict": [[1, 2], [3, 4], [5, 6], [7]],
        "CASE_unwrap_list_dict_twice": [1, 2, 3, 4, 5, 6, 7],
    }


def test_rol_drop(simple_data):
    source = simple_data

    def mapping(m: dict):
        return {
            "CASE_parent_keep": {
                "CASE_curr_drop": {
                    "a": get(m, "notFoundKey", drop_level=ROL.CURRENT),
                    "b": "someValue",
                },
                "CASE_curr_keep": {
                    "id": get(m, "data.patient.id", drop_level=ROL.CURRENT)
                },
            },
            "CASE_list": [
                {"a": get(m, "notFoundKey", drop_level=ROL.PARENT), "b": "someValue"},
                {"a": "someValue", "b": "someValue"},
            ],
        }

    mapper = Mapper(mapping, remove_empty=True)
    res = mapper(source)
    assert res == {
        "CASE_parent_keep": {"CASE_curr_keep": {"id": get(source, "data.patient.id")}}
    }


def test_tuple_unwrapping(nested_data):
    source = nested_data

    def get_jkl() -> dict:
        return {"j": 7, "k": 8, "l": 9}

    def mapping(m: dict) -> dict:
        return {
            ("a", "b", "c"): get(m, "data[0].patient.ints", apply=tuple),
            "nested": {("d", "e", "f"): get(m, "data[1].patient.ints", apply=tuple)},
            ("g", "h", "i"): None,  # This should get removed
            ("j", "k", "l"): get_jkl(),
            ("m", "n"): {"m": 10, "n": None},
        }

    mapper = Mapper(mapping, remove_empty=True)
    res = mapper(source)
    assert res == {
        "a": 1,
        "b": 2,
        "c": 3,
        "nested": {"d": 4, "e": 5, "f": 6},
        "j": 7,
        "k": 8,
        "l": 9,
        "m": 10
    }
