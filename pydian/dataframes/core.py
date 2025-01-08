import re
from typing import Any, Callable

import polars as pl
from result import Err

from .util import JoinExpr, TableExpr, UnionExpr, parse_select_dsl

# from .util import apply_nested_col_list, generate_polars_filter

# Alright. Only support up to 26 tables max at a time. That's it. No exceptions! \s
SOURCE_TABLE_ALIAS = "A"
OTHER_TABLE_ALIASES = "BCDEFGHIJKLMNOPQRSTUVWXYZ"

def select(
    source: pl.DataFrame,
    key: str,
    others: pl.DataFrame | list[pl.DataFrame] | None = None,
    rename: dict[str, str] | Callable[[str], str] | None = None,
) -> pl.DataFrame | Err:
    """
    Selects a subset of a DataFrame. `key` has some convenience functions

    NOTE: By default, the pydian DSL uses `A` as an alias for `source`,
          and `B`, `C`, etc. (up to `Z`) for corresponding dataframes in `others`

    `key` notes:
    - query syntax:
        - "*" -- all columns
        - "a, b, c" -- columns a, b, c (in-order)
        - "a, b : [c > 3]" -- columns a, b where column c > 3
        - "* : [c != 3]" -- all columns where column c != 3
        - "dict_col -> [a, b, c]" -- "dict_col.a, dict_col.b, dict_col.c"
    - join synytax:
        - "a, b from A <- B on [col_name]" -- outer left join onto `col_name`
        - "* from A <> B on [col_name]" -- inner join on `col_name`
        - "* from A ++ B" -- append B into A (whatever columns match)
        - "* from (...) <> B on [col_name]" -- subquery tables
    - groupby synax:
        - "* => groupby[col_name | sum(), max()]"
        - "col_name, other_col from A => groupby[col_name]"
        - "col_name, other_col_sum from A => groupby[col_name | sum()]"
        - "* from A => groupby[col_name, other_col | n_unique(), sum()]

    `rename` is the standard Polars API call and is called at the very end
    """
    colname_expr_list, from_expr_list, table_expr_list = parse_select_dsl(key)

    raise RuntimeError("DEBUG: Parsed successfully!")
    res = source

    # 1. Do joins (if present)
    #    Expect a series of `JoinExpr` which should be applied in-order
    if len(from_expr_list) == 0 and others is not None:
        raise RuntimeError("`others` provided but not used -- please remove to save memory!")
    for from_expr in from_expr_list:
        try:
            # Assume the size of `others` matches things found in expression language
            assert others is not None  # TODO: remove this after testing
            lhs, rhs = _identify_lhs_rhs(res, others, from_expr)
            match from_expr:
                case JoinExpr():
                    # Perform the join
                    res = lhs.join(rhs, on=from_expr.on_cols, how=from_expr.join_type.lower())
                    # TODO: define behavior if there's no change after the join - `Err` or leave alone?
                case UnionExpr():
                    # Perform the union
                    res = lhs.vstack(rhs)
                    # TODO: define `Err` behavior if union fails?
                case _typ:
                    raise RuntimeError(f"Got unexpected operation type: {_typ}")
        except pl.InvalidOperationError:
            return Err(f"Got unexpected operation: {from_expr}")

    # 2. Do select
    try:
        res = res.select(colname_expr_list)
    except pl.ColumnNotFoundError as e:
        return Err(f"Got unexpected column: {e}")

    # 3. Do group operators (if present)
    for table_expr in table_expr_list:
        try:
            match table_expr.op_type:
                case "GROUPBY":
                    res = res.group_by(table_expr.on_cols, maintain_order=True).agg(
                        table_expr.agg_fns
                    )
                case "ORDERBY":
                    res.sort(table_expr.on_cols)

        except pl.InvalidOperationError:
            return Err(f"Failed to apply operation: {table_expr}")

    # Do `rename` if provided and have a dataframe at the end
    if rename and isinstance(res, pl.DataFrame):
        res = res.rename(rename)

    return res


def _identify_lhs_rhs(
    source: pl.DataFrame, others: pl.DataFrame | list[pl.DataFrame], from_expr: JoinExpr | UnionExpr
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Searches through given aliases to get the "correct" table object

    E.g. `A` -> source, `B` -> others[0], `C` -> others[1], ... `Z` -> others[25]
    """
    lhs_idx = OTHER_TABLE_ALIASES.find(from_expr.lhs)
    rhs_idx = OTHER_TABLE_ALIASES.find(from_expr.rhs)
    lhs = source if (from_expr.lhs == SOURCE_TABLE_ALIAS) else others[lhs_idx - 1]
    rhs = source if (from_expr.rhs == SOURCE_TABLE_ALIAS) else others[rhs_idx - 1]
    return (lhs, rhs)


def _try_groupby(
    groupby_clause: str,
    source: pl.DataFrame,
    others: pl.DataFrame | list[pl.DataFrame] | None,  # unused
    keep_order: bool = True,
) -> pl.DataFrame | Err:
    """
    Allows the following shorthands for `group_by`:
    - Use comma-delimited col names
    - Specify aggregators after `|` using list or dict syntax
        - For no aggregator specified, default to `.all()`
        - Explicitly named aggregations will also rename resulting columns
          (adds a suffix of the aggregation name, e.g. `colname_all`)

    Examples:
    - `"groupby[a]"` -- `group_by('a').all()`
    - `"groupby[a, b]"` -- `group_by(['a', 'b']).all()`
    - `"groupby[a | len()]"` -- `group_by('a').agg(pl.len().name.suffix('_len'))`
    - `"groupby[a | mean()]"` -- `group_by('a').agg(pl.mean().name.suffix('_mean'))
    - `"groupby[a | len(), mean()]"` -- `group_by('a').agg([pl.len().name.suffix('_len'), pl.mean().name.suffix('_mean')])

    Supported aggregation functions:
      NOTE: if an agg function is used, then the new column will have the agg name added as a suffix
        AND if an agg function cannot be applied, the column remains unchanged (e.g. std() on a str)
    - `all()`, `len()`, `n_unique()`
    - `sum()`, `mean()`
    - `max()`, `min()`, `median()`
    """
    # NOTE: assumes only one input table, fix with CFG implementation...
    # HACK: handle default the simple way
    DEFAULT_STR = "default"
    # Parse `groupby_clause` str into halfs
    STR_WITHIN_BRACKETS = r"\[([^\]]+)\]"
    bracket_str_list: list[str] = re.findall(STR_WITHIN_BRACKETS, groupby_clause)
    if not bracket_str_list:
        raise RuntimeError(f"Invalid structure for `groupby` clause: {groupby_clause}")
    bracket_str: str = bracket_str_list[0].replace(" ", "")
    if "|" in bracket_str:
        col_names, agg_names = bracket_str.split("|")
    else:
        # Default to `all()`
        col_names, agg_names = bracket_str, DEFAULT_STR

    # Organize appropriate aggregation function
    agg_list = agg_names.split(",")
    # NOTE: `coalesce` keeps the first non-null value. So we try the aggregation, however
    #       if it fails, then we take the `all` aggregation and keep original name to note unchanged
    agg_mapping = {
        DEFAULT_STR: pl.all(),
        "all()": pl.all().name.suffix(
            "_all"
        ),  # If this is explicitly specified, then add the suffix
        "len()": pl.all().len().name.suffix("_len"),
        "n_unique()": pl.n_unique("*").name.suffix("_n_unique"),
        "sum()": pl.all().sum().name.suffix("_sum"),
        "mean()": pl.all().mean().name.suffix("_mean"),
        "max()": pl.all().max().name.suffix("_max"),
        "min()": pl.all().min().name.suffix("_min"),
        "median()": pl.all().median().name.suffix("_median"),
    }
    try:
        mapped_agg_list = [agg_mapping[a] for a in agg_list]
    except KeyError as e:
        raise ValueError(
            f"Unsupported aggregation (if in polars, please open GitHub to suggest): {str(e)}"
        )

    # Perform the groupby
    col_list = col_names.split(",")
    res = source.group_by(col_list, maintain_order=keep_order).agg(mapped_agg_list)

    if res.is_empty():
        return Err("Dataframe after `group_by` is empty")

    return res
