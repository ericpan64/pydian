import re
from collections import defaultdict
from typing import Any, Iterable

import pandas as pd
from result import Err

import pydian.partials as p

from .lib.types import ApplyFunc, ConditionalCheck

REGEX_COMMA_EXCLUDE_BRACKETS = r",(?![^{}]*\})"


def select(
    source: pd.DataFrame,
    key: str,
    default: Any = Err("Default Err: key didn't match"),
    apply: ApplyFunc
    | Iterable[ApplyFunc]
    | dict[str, ApplyFunc | Iterable[ApplyFunc] | Any]
    | None = None,
    only_if: ConditionalCheck | None = None,
    consume: bool = False,
) -> pd.DataFrame | Err:
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

    if not isinstance(res, Err) and only_if:
        res = res if only_if(res) else Err("`only_if` check did not pass")

    if not isinstance(res, Err) and apply:
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
    first: pd.DataFrame, second: pd.DataFrame, on: str | list[str]
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

    return pd.DataFrame(res) if not res.empty else None


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
    drop_cols: str | list[str] | None = None,
    overwrite_cols: dict[str, pd.Series | list[Any]] | None = None,
    add_cols: dict[str | tuple[str, int], pd.Series | list[Any]] | None = None,
    na_default: Any = pd.NA,
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

    # TODO: add "resort", e.g. {"colName": newPositionInt, "colName1": "<-colName2", "colName3": "colName4->", "colName5": "<~>colName6"}
    # TODO: add "extract", e.g. `->` and `+>` conventions from `select`
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
            match cdata:
                case list():
                    n_new_rows = len(cdata)
                    res.loc[0:n_new_rows, cname] = cdata
                case pd.Series():
                    # Drop old column, then reinsert to  prev spot
                    # NOTE: Assumes pd.Series should be exact -- e.g. including name
                    cidx = res.columns.get_loc(cname)
                    res.drop(columns=[cname], inplace=True)
                    new_cname = cdata.name
                    res.insert(cidx, new_cname, cdata)
    if add_cols:
        if not isinstance(add_cols, dict):
            raise ValueError(f"`add_cols` should be a dict, got: {type(add_cols)}")
        for cname, cdata in add_cols.items():  # type: ignore
            # Default to end if adding
            new_idx = len(res.columns)
            if isinstance(cname, tuple):
                cname, new_idx = cname
            # Prevent overwriting an existing column on accident
            if cname in target.columns:
                return None
            # Expect columns smaller than existing df
            if len(cdata) > n_rows:
                return None
            match cdata:
                case list():
                    if len(cdata) < n_rows:
                        cdata.extend([na_default] * (n_rows - len(cdata)))
                    res[cname] = cdata
                case pd.Series():
                    res.insert(new_idx, cdata.name, cdata)
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

    # Extract query from key (if present)
    key = key.replace(" ", "")
    query = None
    if "~" in key:
        key, query = key.split("~")
        query = query.removeprefix("[").removesuffix("]")

    # Extract columns from syntax
    # NOTE: `parsed_col_list` starts with exact user-provided string, then
    #        gets updated in `_generate_nesting_list` to exclude nesting (so matches colname)
    parsed_col_list = re.split(REGEX_COMMA_EXCLUDE_BRACKETS, key)
    nesting_list = _generate_nesting_list(parsed_col_list)

    # Handle "*" case
    # TODO: Handle "*" with other items, e.g. `"*, a -> {b, c}`?
    if parsed_col_list == ["*"]:
        parsed_col_list = source.columns

    try:
        res = source.query(query)[parsed_col_list] if query else source[parsed_col_list]
        res = _apply_nesting_list(res, nesting_list, parsed_col_list)
        # Post-processing checks
        if res.empty:
            res = default
        elif consume:
            # TODO: way to consume just the rows that matched?
            source.drop(columns=parsed_col_list, inplace=True)
    except KeyError:
        res = default

    return res


def _apply_nesting_list(
    source: pd.DataFrame,
    nesting_list: list[str | tuple[bool, list[str] | dict[str, str]] | None],
    parsed_col_list: list[str],
) -> pd.DataFrame:
    # Prevents `SettingWithCopyWarning`, ref: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
    # TODO: does this hog too much extra memory?
    res = source.copy()

    # Apply nesting if applicable
    col_idx_to_del: list[int] = []
    col_to_add: defaultdict[int, list[tuple[bool, pd.Series]]] = defaultdict(list)
    if any(nesting_list):
        for i, nesting in enumerate(nesting_list):  # type: ignore
            cname = parsed_col_list[i]
            match nesting:
                case str():
                    s: pd.Series = res[cname].apply(p.get(nesting))
                    s.name = f"{cname}.{nesting}"
                    res = alter(res, overwrite_cols={cname: s})
                case tuple():
                    keep_col, cobj = nesting
                    if not keep_col:
                        col_idx_to_del.append(i)
                    if isinstance(cobj, list):
                        for nkey in cobj:
                            s: pd.Series = res.loc[:, cname].apply(p.get(nkey))  # type: ignore
                            s.name = f"{cname}.{nkey}"
                            col_to_add[i].append((keep_col, s))
                    elif isinstance(cobj, dict):
                        for new_name, nkey in cobj.items():
                            s: pd.Series = res.loc[:, cname].apply(p.get(nkey))  # type: ignore
                            s.name = new_name
                            col_to_add[i].append((keep_col, s))
                case None:
                    continue

    # Do `tuple` case procecssing
    if col_idx_to_del:
        res.drop(columns=res.columns[col_idx_to_del], inplace=True)
    if col_to_add:
        bump_idx = 0
        for idx, vlist in col_to_add.items():
            # FYI: inserts at the front, so add extra 1 if we kept the original column
            for kcbool, s in vlist:
                res.insert(idx + bump_idx + int(kcbool), s.name, s.values)
                bump_idx += 1

    return res


def _generate_nesting_list(
    parsed_col_list: list[str],
) -> list[str | tuple[bool, list[str] | dict[str, str]] | None]:
    """
    Return whether a specific column index should get nesting logic applied

    For each column, check if:
      1. Column should be extracted and consumed (`->`)
      2. Column should be extracted and kept (`+>`)
      3. Column should be nested into and consumed (exactly once)

    Order matters!

    Not a pure function -- assume `parsed_col_list` might be modified
    """
    nesting_list: list[str | tuple[bool, list[str] | dict[str, str]] | None] = []
    # for i, c in enumerate(parsed_col_list):
    for i, c in enumerate(parsed_col_list):
        # 1. extract, and consume original
        # 2. extract, and keep original
        if ("->" in c) or ("+>" in c):
            keep_col = "+>" in c
            splitter = "+>" if keep_col else "->"
            cname, content = c.split(splitter)
            cobj = _extract_list_or_dict(content)
            if cobj:
                # NOTE: Remove the nesting from `parsed_col_list` for later processing
                parsed_col_list[i] = cname
                nesting_list.append((keep_col, cobj))
            else:
                nesting_list.append(None)
        # 3. nesting, consume and replace
        elif "." in c:
            cname, nesting = c.split(".", maxsplit=1)
            # NOTE: Remove the nesting from `parsed_col_list` for later processing
            parsed_col_list[i] = cname
            nesting_list.append(nesting)
        else:
            nesting_list.append(None)
    return nesting_list


def _extract_list_or_dict(s: str) -> list[str] | dict[str, str] | None:
    """
    Given a string in brackets, tries to extract into set or dict, else None.
    """
    # Check if the string starts and ends with curly braces
    if not (s.startswith("{") and s.endswith("}")):
        return None
    # Remove the curly braces and strip whitespace
    content = s[1:-1].strip()
    res: list[str] | dict[str, str] | None = None
    # Determine if the string is a dictionary (contains ':') or a set
    if ":" in content:
        # Handle dictionary
        try:
            # Split the string into key-value pairs
            items = content.split(",")
            dict_result = {}
            for item in items:
                key, value = item.split(":")
                dict_result[key.strip().strip("'").strip('"')] = value.strip().strip("'").strip('"')
            res = dict_result
        except ValueError:
            res = None
    else:
        # Handle set (return as a list to preserve ordering)
        res = [x.strip().strip("'").strip('"') for x in content.split(",")]
    return res
