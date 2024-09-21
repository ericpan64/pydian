from collections.abc import Collection
from itertools import chain
from typing import Any, Callable, Iterable, Sequence, TypeVar

import jmespath

from .types import DROP, KEEP

DL = TypeVar("DL", dict[str, Any], list[Any], Any)


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


def get_keys_containing_class(source: dict[str, Any], cls: type, key_prefix: str = "") -> set[str]:
    """
    Recursively finds all keys where a DROP object is found.
    """
    res = set()
    for k, v in source.items():
        curr_key = f"{key_prefix}.{k}" if key_prefix != "" else k
        match v:
            case cls():  # type: ignore
                res.add(curr_key)
            case dict():
                res |= get_keys_containing_class(v, cls, curr_key)
            case list():
                for i, item in enumerate(v):
                    indexed_keypath = f"{curr_key}[{i}]"
                    if isinstance(item, cls):
                        res.add(indexed_keypath)
                    elif isinstance(item, dict):
                        res |= get_keys_containing_class(item, cls, indexed_keypath)
    return res


def flatten_list(res: list[list[Any]]) -> list[Any]:
    """
    Flattens a list-of-list
    E.g. Given:    [[1, 2, 3], [4, 5, 6], None, [7, 8, 9]]
         Returns:  [1, 2, 3, 4, 5, 6, 7, 8, 9]
    """
    if res_without_nones := [l for l in res if (l is not None) and (isinstance(l, list))]:
        res = list(chain.from_iterable(res_without_nones))
        # Handle nested case
        res = flatten_list(res)
    return res


def default_dsl(source: dict[str, Any] | list[Any], key: str):
    """
    Specifies a DSL (domain-specific language) to use when running `get`

    Here, we redefine the `jmespath.search` to be consistent with argument ordering in the repo
    """
    return jmespath.search(key, source)


def drop_keys(source: dict[str, Any], keys_to_drop: Iterable[str]) -> dict[str, Any]:
    """
    Returns the dictionary with the requested keys set to `None`.

    If a key is a duplicate, then lookup fails so that key is skipped.

    DROP values are checked and handled here.
    """
    res = source
    seen_keys = set()
    for key in keys_to_drop:
        curr_keypath = get_tokenized_keypath(key)
        if curr_keypath not in seen_keys:
            if v := _nested_get(res, key):
                # Check if value has a DROP object
                if isinstance(v, DROP):
                    # If "out of bounds", raise an error
                    if v.value > 0 or -1 * v.value > len(curr_keypath):
                        raise RuntimeError(f"Error: DROP level {v} at {key} is invalid")
                    curr_keypath = curr_keypath[: v.value]
                    # Handle case for dropping entire object
                    if len(curr_keypath) == 0:
                        return dict()
                if updated := _nested_set(res, curr_keypath, None):
                    res = updated
                seen_keys.add(curr_keypath)
        else:
            seen_keys.add(curr_keypath)
    return res


def impute_enum_values(source: dict[str, Any], keys_to_impute: set[str]) -> dict[str, Any]:
    """
    Returns the dictionary with the Enum values set to their corresponding `.value`
    """
    res = source
    for key in keys_to_impute:
        curr_val = _nested_get(res, key)
        if isinstance(curr_val, KEEP):
            literal_val = curr_val.value
            res = _nested_set(res, get_tokenized_keypath(key), literal_val)  # type: ignore
    return res


def get_tokenized_keypath(key: str) -> tuple[str | int, ...]:
    """
    Returns a keypath with str and ints separated. Prefer tuples so it is hashable.

    E.g.: "a[0].b[-1].c" -> ("a", 0, "b", -1, "c")
    """
    tokenized_key = key.replace("[", ".").replace("]", "")
    keypath = tokenized_key.split(".")
    return tuple(int(k) if k.removeprefix("-").isnumeric() else k for k in keypath)


def _nested_get(
    source: dict[str, Any] | list[Any],
    key: str | Any,
    default: Any = None,
    dsl_fn: Callable[[dict[str, Any] | list[Any], Any], Any] = default_dsl,
) -> Any:
    """
    Expects `.`-delimited string and tries to get the item in the dict.

    If using pydian defaults, the following benefits apply:
    - Tuple support

    If you use a custom `dsl_fn`, then logic is entrusted to that function (wgpcgr).
    """
    # Assume `key: str`. If not, then trust the custom `dsl_fn` to handle it
    # Handle tuple syntax (if they ask for a tuple, return a tuple)
    if isinstance(key, str) and ("(" in key and ")" in key):
        key = key.replace("(", "[").replace(")", "]")
        res = dsl_fn(source, key)
        if isinstance(res, list):
            res = tuple(res)
    else:
        res = dsl_fn(source, key)

    # DSL-independent cleanup
    if isinstance(res, list):
        res = [r if r is not None else default for r in res]
    if res is None:
        res = default

    return res


def _nested_set(
    source: dict[str, Any], tokenized_key_list: Sequence[str | int], target: Any
) -> dict[str, Any] | None:
    """
    Returns a copy of source with the replace if successful, else None.
    """
    res: Any = source
    try:
        for k in tokenized_key_list[:-1]:
            res = res[k]
        res[tokenized_key_list[-1]] = target
    except IndexError:
        return None
    return source
