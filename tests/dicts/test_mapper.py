from typing import Any

import pytest

from pydian import Mapper, get
from pydian.dicts.mapper import mapping_context
from pydian.lib.types import DROP, KEEP


def test_drop(simple_data: dict[str, Any]) -> None:
    source = simple_data

    def mapping(d: dict[str, Any]) -> dict[str, Any]:
        return {
            "CASE_parent_keep": {
                "CASE_curr_drop": {
                    "a": DROP.THIS_OBJECT,
                    "b": "someValue",
                },
                "CASE_curr_keep": {"id": get(d, "data.patient.id")},
            },
            "CASE_list": [DROP.THIS_OBJECT],
            "CASE_list_of_objects": [
                {"a": DROP.PARENT, "b": "someValue"},
                {"a": "someValue", "b": "someValue"},
            ],
        }

    mapper = Mapper(mapping, remove_empty=True)
    res = mapper(source)
    assert res == {"CASE_parent_keep": {"CASE_curr_keep": {"id": get(source, "data.patient.id")}}}


def test_drop_out_of_bounds() -> None:
    source: dict[str, Any] = {}

    def mapping(_: dict[str, Any]) -> dict[str, Any]:
        return {"parent": {"CASE_no_grandparent": DROP.GREATGRANDPARENT}}

    mapper = Mapper(mapping)
    with pytest.raises(RuntimeError):
        _ = mapper(source)


def test_drop_exact_level() -> None:
    source: dict[str, Any] = {}

    def mapping(_: dict[str, Any]) -> dict[str, Any]:
        return {
            "parent": {"CASE_has_parent_object": DROP.PARENT},
            "other_data": 123,
        }

    mapper = Mapper(mapping)
    res = mapper(source)
    assert res == {}


def test_drop_repeat() -> None:
    source: dict[str, Any] = {}

    def mapping(_: dict[str, Any]) -> dict[str, Any]:
        return {
            "dropped_direct": [DROP.THIS_OBJECT, DROP.THIS_OBJECT],
            "also_dropped": [{"parent_key": DROP.PARENT}, DROP.THIS_OBJECT],
            "partially_dropped": [
                "first_kept",
                {"second_dropped": DROP.THIS_OBJECT},
                "third_kept",
                {"fourth_dropped": DROP.THIS_OBJECT},
            ],
        }

    mapper = Mapper(mapping)
    res = mapper(source)
    assert res == {"partially_dropped": ["first_kept", "third_kept"]}


def test_keep_empty_value() -> None:
    source: dict[str, Any] = {}

    def mapping(_: dict[str, Any]) -> dict[str, Any]:
        return {
            "empty_vals": [KEEP({}), KEEP([]), KEEP(""), KEEP(None)],
            "nested_vals": {
                "dict": KEEP({}),
                "list": KEEP([]),
                "str": KEEP(""),
                "none": KEEP(None),
                "other_static_val": "Abc",
            },
            "static_val": "Def",
            "empty_list": KEEP([]),
            "removed_empty_list": [],
        }

    mapper = Mapper(mapping)
    res = mapper(source)
    assert KEEP({}).value == dict()
    assert KEEP([]).value == list()
    assert KEEP("").value == ""
    assert KEEP(None).value == None
    assert res == {
        "empty_vals": [{}, [], "", None],
        "nested_vals": {"dict": {}, "list": [], "str": "", "none": None, "other_static_val": "Abc"},
        "static_val": "Def",
        "empty_list": [],
    }


def test_strict(simple_data: dict[str, Any]) -> None:
    source = simple_data

    # Test `strict` flag (independent of `get`)
    def mapping(d: dict[str, Any]) -> dict[str, Any]:
        return {
            "CASE_parent_keep": {
                "CASE_curr_drop": {
                    "a": DROP.THIS_OBJECT,
                    "b": "someValue",
                },
                "CASE_curr_keep": {"id": get(d, "data.patient.id")},
            },
            "CASE_missing": get(d, "key.nope.not.there"),
        }

    # Strict case
    with mapping_context(strict=True):
        mapper = Mapper(mapping)
        with pytest.raises(ValueError) as exc_info:
            mapper(source)

    # Not strict case
    assert mapper(source) == {
        "CASE_parent_keep": {"CASE_curr_keep": {"id": get(source, "data.patient.id")}}
    }

    # Test `strict` flag with specific `get` calls
    def get_mapping(d: dict[str, Any]) -> dict[str, Any]:
        return {
            "CASE_parent_keep": {
                "CASE_curr_drop": {
                    "a": DROP.THIS_OBJECT,
                    "b": "someValue",
                },
                "CASE_curr_keep": {"id": get(d, "data.patient.id")},
            },
            "CASE_missing": get(d, "key.nope.not.there"),
        }

    get_mapper = Mapper(get_mapping)
    with mapping_context(strict=True):
        with pytest.raises(ValueError) as exc_info:
            get_mapper(source)
    get_mapper(source)  # TODO: Add assert


def test_strict_deliberate_none() -> None:
    source = {
        "has_None": None,
        "nested_None": {"has_None": None, "has_value": "value"},
        "nested_list_None": {
            "some_list": [
                {"has_None": None},
                "value",
                None,
            ],
        },
    }

    def mapping_success(d: dict[str, Any]) -> dict[str, Any]:
        return {
            "CASE_keep_None": get(d, "has_None"),
            "CASE_keep_None_nested": get(d, "nested_None.has_None"),
            "CASE_keep_None_list": get(d, "nested_list_None.some_list[-1]"),
            "CASE_keep_None_list_nested": get(d, "nested_list_None.some_list[0].has_None"),
        }

    mapper = Mapper(mapping_success, remove_empty=False)

    with mapping_context(strict=True):
        assert mapper(source) == {
            "CASE_keep_None": None,
            "CASE_keep_None_nested": None,
            "CASE_keep_None_list": None,
            "CASE_keep_None_list_nested": None,
        }

    def mapping_err(d: dict[str, Any]) -> dict[str, Any]:
        return {"CASE_keep_None": get(d, "has_None"), "CASE_throw_err": get(d, "key.not.found")}

    err_mapper = Mapper(mapping_err)

    with mapping_context(strict=True):
        with pytest.raises(ValueError) as exc_info:
            err_mapper(source)
