from typing import Any

import pandas as pd
import pytest


def simple_nested_list() -> list[dict[str, Any]]:
    return [
        {"patient": {"id": "abc123", "active": True}},
        {"patient": {"id": "def456", "active": True}},
        {"patient": {"id": "ghi789", "active": False}},
    ]


def deep_nested_list() -> list[dict[str, Any]]:
    return [
        {
            "patient": {
                "id": "abc123",
                "active": True,
                "ints": [1, 2, 3],
                "dict": {"char": "a", "inner": {"msg": "A!"}},
                "dicts": [
                    {"num": 1, "text": "one", "inner": {"msg": "One!"}},
                    {"num": 2, "text": "two", "inner": {"msg": "Two!"}},
                ],
            }
        },
        {
            "patient": {
                "id": "def456",
                "active": False,
                "ints": [4, 5, 6],
                "dict": {"char": "b", "inner": {"msg": "B!"}},
                "dicts": [
                    {"num": 3, "text": "three", "inner": {"msg": "Three!"}},
                    {"num": 4, "text": "four", "inner": {"msg": "Four!"}},
                ],
            }
        },
        {
            "patient": {
                "id": "ghi789",
                "active": True,
                "ints": [7, 8, 9],
                "dict": {"char": "c", "inner": {"msg": "C!"}},
                "dicts": [
                    {"num": 5, "text": "five", "inner": {"msg": "Five!"}},
                    {"num": 6, "text": "six", "inner": {"msg": "Six!"}},
                ],
            }
        },
        {
            "patient": {
                "id": "jkl101112",
                "active": True,
                # 'ints' is deliberately missing
                "dict": {"char": "d", "inner": {"msg": "D!"}},
                # `dicts` is deliberately len=1 instead of len=2
                "dicts": [{"num": 7, "text": "seven", "inner": {"msg": "Seven!"}}],
            }
        },
    ]


@pytest.fixture(scope="function")
def list_data() -> list[Any]:
    return simple_nested_list()


@pytest.fixture(scope="function")
def simple_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "a": [0, 1, 2, 3, 4, 5],
            "b": ["q", "w", "e", "r", "t", "y"],
            "c": [True, False, True, False, False, True],
            "d": [pd.NA, None, None, pd.NA, pd.NA, None],
        }
    )


@pytest.fixture(scope="function")
def nested_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            # Wrap in `pd.Series` to allow for different lengths, backfill with `NaN`
            "simple_nesting": pd.Series(simple_nested_list()),  # len: 3
            "deep_nesting": pd.Series(deep_nested_list()),  # len: 4
            "a": pd.Series([0, 1, 2, 3, 4]),  # len: 5
        }
    )


@pytest.fixture(scope="function")
def simple_data() -> dict[str, Any]:
    return {
        "data": {"patient": {"id": "abc123", "active": True}},
        "list_data": simple_nested_list(),
    }


@pytest.fixture(scope="function")
def nested_data() -> dict[str, Any]:
    return {"data": deep_nested_list()}
