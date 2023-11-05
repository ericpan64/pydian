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

    # # Only consume if there was a change
    # if consume:
    #     # Only drop rows that were included in the left join
    #     matched_rows = select(res, f"{','.join(on)} ~ [_merge == 'both']")  # type: ignore
    #     # TODO: making assumption on indices here, is this a problem?
    #     # TODO: ^Yes that was a problem, good intuition! Have to match on the _value_
    #     if not matched_rows.empty:
    #         second.drop(index=matched_rows.index, inplace=True)  # type: ignore

    res.drop("_merge", axis=1, inplace=True)

    return res if not res.empty else None


def inner_join(
    first: pd.DataFrame, second: pd.DataFrame, on: str | list[str]
) -> pd.DataFrame | None:
    """
    Applies an inner join. Returns `None` if nothing was joined
    """
    try:
        _pre_merge_checks(first, second, on)
    except KeyError:
        return None

    res = first.merge(second, how="inner", on=on, indicator=False)

    return res if not res.empty else None


def insert(
    into: pd.DataFrame,
    rows=pd.DataFrame | list[dict[str, Any]],
    na_default: Any = pd.NA,
    consume: bool = False,
) -> pd.DataFrame | None:
    """
    Inserts rows into the end of the DataFrame

    For a row, if a value is not specified it will be filled with the specified default

    If the insert operation cannot be done (e.g. incompatible columns), returns `None`
    """
    if isinstance(rows, list):
        rows = pd.DataFrame(rows)
    rows.fillna(na_default, inplace=True)
    try:
        _check_assumptions([into, rows])
        if not set(into.columns).intersection(set(rows.columns)):
            raise ValueError("Input rows have no overlapping columns, skip insert")
        res = pd.concat([into, rows], ignore_index=True)
        if consume:
            # Drop all of the inserted rows
            rows.drop(index=rows.index, inplace=True)
    except:
        res = None
    return res


def alter(
    target: pd.DataFrame,
    overwrite_cols: dict[str, list[Any]] | None = None,
    add_cols: dict[str, list[Any]] | None = None,
    na_default: Any = pd.NA,
    drop_cols: str | list[str] | None = None,
    consume: bool = False,
) -> pd.DataFrame | None:
    """
    Returns a new copy of a modified database, or `None` if modifications aren't done. E.g.:
    - If the column already exists when trying to add a new one
    - If the length of a new column is larger than the target dataframe
    - ... etc.

    Operations (in-order):
    - `drop_cols` should be comma-delimited or provide the list of columns
    - `overwrite_cols` should replace existing columns with provided data (up to that point)
    - `add_cols` should map the new column name to initial data (missing values will use `na_default`)
    """
    _check_assumptions(target)
    res = target
    n_rows, _ = res.shape
    if drop_cols:
        if isinstance(drop_cols, str):
            drop_cols = drop_cols.replace(" ", "").split(",")
        res = res.drop(columns=drop_cols)
    if overwrite_cols:
        if not isinstance(overwrite_cols, dict):
            raise ValueError(f"`overwrite_cols` should be a dict, got: {type(add_cols)}")
        for cname, cdata in overwrite_cols.items():
            # Expect column to be there
            if cname not in target.columns:
                return None
            # Expect columns smaller than existing df
            if len(cdata) > n_rows:
                return None
            n_new_rows = len(cdata)
            res.loc[0:n_new_rows, cname] = cdata
    if add_cols:
        if not isinstance(add_cols, dict):
            raise ValueError(f"`add_cols` should be a dict, got: {type(add_cols)}")
        for cname, cdata in add_cols.items():
            # Prevent overwriting an existing column on accident
            if cname in target.columns:
                return None
            # Expect columns smaller than existing df
            if len(cdata) > n_rows:
                return None
            cdata.extend([na_default] * (n_rows - len(cdata)))
            res[cname] = cdata
    if consume:
        target.drop(columns=target.columns, inplace=True)
    return res


def _check_assumptions(source: pd.DataFrame | Iterable[pd.DataFrame]) -> None:
    if isinstance(source, pd.DataFrame):
        source = (source,)
    for df in source:
        ## Check for column names that are `str`
        col_types = {type(c) for c in df.columns}
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
    _check_assumptions([first, second])
    if isinstance(on, str):
        on = [on]
    for c in on:
        if not (c in first.columns and c in second.columns):
            raise KeyError(f"Proposed key {c} is not in either column!")


def _nested_select(
    source: pd.DataFrame, key: str, default: Any, consume: bool
) -> pd.DataFrame | Any:
    res = None
    # Get operations from key
    key = key.replace(" ", "")
    query = None
    if "~" in key:
        key, query = key.split("~")
        query = query.removeprefix("[").removesuffix("]")

    # Get columns from syntax
    parsed_col_list = list(key.split(","))
    # parsed_col_list = key.replace(" ", "").split(",")
    if parsed_col_list == ["*"]:
        parsed_col_list = source.columns
    try:
        res = source.query(query)[parsed_col_list] if query else source[parsed_col_list]
        if res.empty:
            res = default
        elif consume:
            # TODO: way to consume just the rows that matched?
            source.drop(columns=parsed_col_list, inplace=True)
    except KeyError:
        res = default
    return res
