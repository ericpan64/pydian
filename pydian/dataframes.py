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
    | dict[str, ApplyFunc | Iterable[ApplyFunc] | Any]
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
    - _Order matters_


    - `consume`: Remove the original data from the dataframe from memory
    """
    _check_assumptions(source)

    res = _nested_select(source, key, default, consume)

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


def left_join(
    first: pd.DataFrame, second: pd.DataFrame, on: str | list[str], consume: bool = False
) -> pd.DataFrame | None:
    """
    Applies a left join

    A left join resulting in no change or an empty database results in None
    """
    try:
        _pre_merge_checks(first, second, on)
    except KeyError:
        return None

    res = first.merge(second, how="left", on=on, indicator=True)

    # If there were no matches, then return `None`
    if select(res, "_merge", apply=[p.iloc(None, 0), set]) == {"left_only"}:
        return None

    # Only consume if there was a change
    if consume:
        # Only drop rows that were included in the left join
        # TODO: MAKE SELECT SYNTAX MAGIC MASKING! This doesn't work (yet)
        matched_rows = select(res, ",".join(on.replace("_merge", "_merge[?_merge == 'both']")))  # type: ignore
        # TODO: making assumption on indices here, is this a problem?
        second.drop(index=matched_rows.index, inplace=True)  # type: ignore

    res.drop("_merge", axis=1, inplace=True)

    return res if not res.empty else None


def inner_join(
    first: pd.DataFrame, second: pd.DataFrame, on: str | list[str], consume: bool = False
) -> pd.DataFrame | None:
    """
    Applies an inner join
    """
    try:
        _pre_merge_checks(first, second, on)
    except KeyError:
        return None

    res = first.merge(second, how="inner", on=on, indicator=True)

    # Only consume if a join happened
    if consume and not res.empty:
        # Create a boolean mask indicating whether each row in `first` is also present in `second`
        fmask = first.isin(second.to_dict(orient="list")).all(axis=1)
        smask = second.isin(first.to_dict(orient="list")).all(axis=1)
        first.drop(first[fmask].index, inplace=True)
        second.drop(second[smask].index, inplace=True)

    return res if not res.empty else None


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


def _pre_merge_checks(first: pd.DataFrame, second: pd.DataFrame, on: str | list[str]) -> None:
    # If _any_ of the provided indices aren't there, return `None`
    if isinstance(on, str):
        on = [on]
    for c in on:
        if not (c in first.columns and c in second.columns):
            raise KeyError(f"Proposed key {c} is not in either column!")


def _nested_select(
    source: pd.DataFrame, key: str, default: Any, consume: bool
) -> pd.DataFrame | Any:
    res = None
    # Get columns from syntax
    parsed_col_list = key.replace(" ", "").split(",")
    if parsed_col_list == ["*"]:
        parsed_col_list = source.columns
    try:
        res = source[parsed_col_list]
        if res.empty:
            res = default
        elif consume:
            source.drop(columns=parsed_col_list, inplace=True)
    except KeyError:
        res = default
    return res
