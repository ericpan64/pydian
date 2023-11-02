from typing import Any, Iterable

import pandas as pd

import pydian.partials as p

from .lib.types import ApplyFunc, ConditionalCheck


def select(
    source: pd.DataFrame,
    key: str,
    default: Any = None,
    apply: ApplyFunc
    | Iterable[ApplyFunc]
    | dict[str, ApplyFunc | Iterable[ApplyFunc]]
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


    - `consume`: Remove the original data from the dataframe from memory
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
    # TODO: Case where some valid columns, and some invalid?
    except KeyError:
        res = default
    if consume:
        source.drop(columns=parsed_col_list, inplace=True)

    if res is not None and only_if:
        res = res if only_if(res) else None

    if res is not None and apply:
        if isinstance(apply, dict) and isinstance(res, pd.DataFrame):
            # Each key is a column name
            #  and each value contains a list of operations
            for k, v in apply.items():
                # For each column, apply the list of operations (v) to each value
                res[k] = res[k].apply(p.do(_try_apply, v, key))
        else:
            res = _try_apply(res, apply, key)  # type: ignore

    return res


def _check_assumptions(source: pd.DataFrame) -> None:
    ## Check for column names that are `str`
    col_types = {type(c) for c in source.columns}
    if col_types != {str}:
        raise ValueError(f"Column headers need to be `str`, got: {col_types}")


def _try_apply(source: Any, apply: ApplyFunc | Iterable[ApplyFunc], key: str) -> Any:
    res = source
    if not isinstance(apply, Iterable):
        apply = (apply,)
    for fn in apply:
        try:
            res = fn(res)
        except Exception as e:
            raise RuntimeError(f"`apply` call {fn} failed for value: {res} with key: {key}, {e}")
        if res is None:
            break
    return res
