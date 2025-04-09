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
    try:
        colname_expr_list, from_expr_list, table_expr_list = parse_select_dsl(key)
        res = source

        # 1. Do joins (if present)
        if len(from_expr_list) == 0 and others is not None:
            raise RuntimeError("`others` provided but not used -- please remove to save memory!")
        for from_expr in from_expr_list:
            # Assume the size of `others` matches things found in expression language
            assert others is not None  # TODO: remove this after testing
            lhs, rhs = _identify_lhs_rhs(res, others, from_expr)
            match from_expr:
                case JoinExpr():
                    # Check if either DataFrame is empty
                    if lhs.height == 0 or rhs.height == 0:
                        return Err("Cannot join with empty DataFrame")

                    # Validate join columns exist in both tables
                    for col in from_expr.on_cols:
                        if col not in lhs.columns:
                            return Err(f"Join column '{col}' not found in left table")
                        if col not in rhs.columns:
                            return Err(f"Join column '{col}' not found in right table")

                    # Ensure join columns have compatible types
                    for col in from_expr.on_cols:
                        lhs_type = lhs[col].dtype
                        rhs_type = rhs[col].dtype
                        if lhs_type != rhs_type:
                            # Try to cast to compatible type
                            if lhs_type.is_numeric() and rhs_type.is_numeric():
                                # Cast to higher precision
                                if lhs_type.is_float() or rhs_type.is_float():
                                    lhs = lhs.with_columns(pl.col(col).cast(pl.Float64))
                                    rhs = rhs.with_columns(pl.col(col).cast(pl.Float64))
                                else:
                                    lhs = lhs.with_columns(pl.col(col).cast(pl.Int64))
                                    rhs = rhs.with_columns(pl.col(col).cast(pl.Int64))
                            else:
                                # Convert to string for non-numeric types
                                lhs = lhs.with_columns(pl.col(col).cast(pl.Utf8))
                                rhs = rhs.with_columns(pl.col(col).cast(pl.Utf8))

                    try:
                        res = lhs.join(rhs, on=from_expr.on_cols, how=from_expr.join_type.lower(), coalesce=True)
                    except pl.StructFieldNotFoundError as e:
                        return Err(f"Join failed: {str(e)}")
                case UnionExpr():
                    # Check for incompatible columns
                    lhs_cols = set(lhs.columns)
                    rhs_cols = set(rhs.columns)
                    if not (lhs_cols.issubset(rhs_cols) or rhs_cols.issubset(lhs_cols)):
                        return Err("Cannot union tables with incompatible columns")
                    
                    # Ensure both dataframes have compatible schemas
                    all_cols = sorted(lhs_cols.union(rhs_cols))
                    
                    # Create schema mapping for each column
                    schema = {}
                    for col in all_cols:
                        if col in lhs_cols and col in rhs_cols:
                            lhs_type = lhs[col].dtype
                            rhs_type = rhs[col].dtype
                            if lhs_type != rhs_type:
                                # Use most permissive type
                                if lhs_type.is_numeric() and rhs_type.is_numeric():
                                    schema[col] = pl.Float64
                                else:
                                    schema[col] = pl.Utf8
                        elif col in lhs_cols:
                            schema[col] = lhs[col].dtype
                        else:
                            schema[col] = rhs[col].dtype
                    
                    # Cast columns to compatible types
                    lhs_filled = lhs.with_columns([
                        pl.lit(None).cast(schema[col]).alias(col)
                        for col in all_cols
                        if col not in lhs_cols
                    ])
                    rhs_filled = rhs.with_columns([
                        pl.lit(None).cast(schema[col]).alias(col)
                        for col in all_cols
                        if col not in rhs_cols
                    ])
                    
                    # Cast existing columns if needed
                    for col in all_cols:
                        if col in schema:
                            if col in lhs_cols:
                                lhs_filled = lhs_filled.with_columns(pl.col(col).cast(schema[col]))
                            if col in rhs_cols:
                                rhs_filled = rhs_filled.with_columns(pl.col(col).cast(schema[col]))
                    
                    # Perform the union with aligned columns
                    res = lhs_filled.select(all_cols).vstack(rhs_filled.select(all_cols))
                case _typ:
                    raise RuntimeError(f"Got unexpected operation type: {_typ}")

        # 2. Do select
        try:
            res = res.select(colname_expr_list)
        except pl.StructFieldNotFoundError as e:
            return Err(f"Column selection failed: {str(e)}")

        # 3. Do group operators (if present)
        for table_expr in table_expr_list:
            match table_expr.op_type:
                case "GROUPBY":
                    # Validate groupby columns exist
                    missing_cols = [col for col in table_expr.on_cols if col not in res.columns]
                    if missing_cols:
                        return Err(f"Groupby columns not found: {', '.join(missing_cols)}")
                    
                    # Apply groupby with aggregation functions
                    try:
                        if table_expr.agg_fns:
                            # Apply aggregation functions to all non-groupby columns
                            agg_exprs = []
                            for col in res.columns:
                                if col not in table_expr.on_cols:
                                    for agg_fn in table_expr.agg_fns:
                                        if isinstance(agg_fn, str):
                                            # Use the aggregation function name as suffix
                                            agg_exprs.append(pl.col(col).agg_groups().alias(f"{col}_{agg_fn}"))
                                        else:
                                            # For custom expressions, use the column name as is
                                            agg_exprs.append(pl.col(col).agg_groups())
                            res = res.group_by(table_expr.on_cols, maintain_order=True).agg(agg_exprs)
                        else:
                            # Default to all() if no aggregation functions specified
                            res = res.group_by(table_expr.on_cols, maintain_order=True).agg(pl.all())
                    except pl.ColumnNotFoundError as e:
                        return Err(f"Got unexpected column: {e}")
                    except pl.StructFieldNotFoundError as e:
                        return Err(f"Group operation failed: {str(e)}")
                case "ORDERBY":
                    # Validate orderby columns exist
                    missing_cols = [col for col in table_expr.on_cols if col not in res.columns]
                    if missing_cols:
                        return Err(f"Orderby columns not found: {', '.join(missing_cols)}")
                    res = res.sort(table_expr.on_cols)
                case _:
                    raise RuntimeError(f"Got unexpected operation type: {table_expr.op_type}")

        # Do `rename` if provided and have a dataframe at the end
        if rename and isinstance(res, pl.DataFrame):
            res = res.rename(rename)

        return res

    except pl.ColumnNotFoundError as e:
        return Err(f"Got unexpected column: {e}")
    except pl.InvalidOperationError as e:
        return Err(f"Got unexpected operation: {e}")
    except (RuntimeError, ValueError, AssertionError) as e:
        return Err(str(e))


def _identify_lhs_rhs(
    source: pl.DataFrame, others: pl.DataFrame | list[pl.DataFrame], from_expr: JoinExpr | UnionExpr
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Searches through given aliases to get the "correct" table object

    E.g. `A` -> source, `B` -> others[0], `C` -> others[1], ... `Z` -> others[25]
    """
    if isinstance(others, pl.DataFrame):
        others = [others]
    lhs_idx = OTHER_TABLE_ALIASES.find(from_expr.lhs)
    rhs_idx = OTHER_TABLE_ALIASES.find(from_expr.rhs)
    lhs = source if (from_expr.lhs == SOURCE_TABLE_ALIAS) else others[lhs_idx - 1]
    rhs = source if (from_expr.rhs == SOURCE_TABLE_ALIAS) else others[rhs_idx - 1]
    return (lhs, rhs)
