from typing import Any, Callable, Iterable

import pandas as pd

from .lib.types import ApplyFunc, ConditionalCheck


def select(
    source: pd.DataFrame,
    key: str,
    default: Any = None,
    apply: ApplyFunc | Iterable[ApplyFunc]
    # | dict[str, ApplyFunc | Iterable[ApplyFunc]]
    | None = None,
    only_if: ConditionalCheck | None = None,
    consume: bool = False,
) -> pd.DataFrame | None:
    """
    Gets a subset of a DataFrame. The following conditions apply:
    1. Columns must have names, otherwise an exception will be raised
    2. Index names will be ignored: a row is identified by its 0-indexed position

    PURE FUNCTION: `source` is not modified. This makes memory management important

    `key` notes:
    - Strings represent columns, int represent rows


    - `consume`: Remove the original dataframe from memory
    """
    _check_assumptions(source)

    # Get columns from syntax
    parsed_col_list = key.replace(" ", "").split(",")
    if parsed_col_list == ["*"]:
        parsed_col_list = source.columns
    try:
        res = source[parsed_col_list]
        if res.empty:
            res = default
    except KeyError:
        res = default

    if res is not None and only_if:
        res = res if only_if(res) else None

    if res is not None and apply:
        if not isinstance(apply, Iterable):
            apply = (apply,)
        for fn in apply:
            try:
                res = fn(res)
            except Exception as e:
                raise RuntimeError(f"`apply` call {fn} failed for value: {res} at key: {key}, {e}")
            if res is None:
                break

    return res


def _check_assumptions(source: pd.DataFrame) -> None:
    ## Check for column names that are `str`
    col_types = {type(c) for c in source.columns}
    if col_types != {str}:
        raise ValueError(f"Column headers need to be `str`, got: {col_types}")
