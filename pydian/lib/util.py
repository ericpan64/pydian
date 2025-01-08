"""
Utility functions that can be used across modules. Meant to be for primitive types
"""

from collections.abc import Collection
from typing import Any, Generator, TypeVar

from .types import KEEP

DL = TypeVar("DL", dict[str, Any], list[Any], Any)
LT = TypeVar("LT", list[Any], tuple[Any], Any)


def remove_empty_values(input: DL) -> DL:
    """
    Recursively removes "empty" objects (`None` and/or objects only containing `None` values).
    """
    if isinstance(input, list):
        return [remove_empty_values(v) for v in input if has_content(v)]
    elif isinstance(input, dict):
        return {k: remove_empty_values(v) for k, v in input.items() if has_content(v)}
    return input


def has_content(obj: Any) -> bool:
    """
    Checks if the object has "content" (a non-`None` value), and/or contains at least one item with "content".
    """
    res = obj is not None
    if res and isinstance(obj, Collection):
        res = len(obj) > 0
        # If has items, recursively check if those items have content.
        #   A case has content if at least one inner item has content.
        if isinstance(obj, list):
            res = any(has_content(item) for item in obj)
        elif isinstance(obj, dict):
            res = any(has_content(item) for item in obj.values())
    return res


def flatten_sequence(res: LT) -> Generator:
    """
    Recursively flattens nested lists and tuples

    NOTE: This also unwraps `KEEP` values (pydian-specific)

    E.g. Given:    [[1, 2, 3], [4, 5, 6], None, [7, 8, 9]]
         Returns:  Generator([1, 2, 3, 4, 5, 6, 7, 8, 9])
    """
    for item in res:
        if isinstance(item, (list, tuple)):
            # NOTE: Cannot use `Sequence` type, since a `str` is a sequence and recurses infinitely
            yield from flatten_sequence(item)
        elif isinstance(item, KEEP):
            yield item.value
        elif item is not None:
            yield item
