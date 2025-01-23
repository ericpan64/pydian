from typing import Generator

from pydian.lib.util import flatten_sequence, remove_empty_values


def test_remove_empty_values() -> None:
    # List cases
    assert remove_empty_values([[], {}]) == []
    assert remove_empty_values(["a", [], {}, "", None]) == ["a"]
    # Dict cases
    assert remove_empty_values({"empty_list": [], "empty_dict": {}}) == {}
    assert remove_empty_values({"empty_list": [], "empty_dict": {}, "a": "b"}) == {"a": "b"}
    # Nested cases
    assert remove_empty_values([{}, ["", None], [{"empty": {"dict": {"key": None}}}]]) == []
    assert remove_empty_values({"empty_list": [{}, {}, {}], "empty_dict": {"someKey": {}}}) == {}


def test_flatten_sequence() -> None:
    assert list(flatten_sequence([[1], [2], [3]])) == [1, 2, 3]
    assert tuple(flatten_sequence([[1, 2], [3, 4], [5, 6]])) == (1, 2, 3, 4, 5, 6)
    assert list(flatten_sequence([[[1], 2], [[3], 4], [[5], 6]])) == [1, 2, 3, 4, 5, 6]
    assert list(flatten_sequence([[[1], [2]], [[3], [4]], [[5], [6]]])) == [1, 2, 3, 4, 5, 6]

    # Ignore string case (avoid recursion error)
    assert isinstance(flatten_sequence("abc"), Generator)
