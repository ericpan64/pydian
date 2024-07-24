import ast
import re
from collections import defaultdict
from typing import Any, Callable, Iterable

import polars as pl
from result import Err

import pydian.partials as p

REGEX_COMMA_EXCLUDE_BRACKETS = r",(?![^{}]*\})"


def select(
    source: pl.DataFrame,
    key: str,
    default: Any = Err("Default Err: key didn't match"),
    consume: bool = False,
    rename: dict[str, str] | Callable[[str], str] | None = None,
) -> pl.DataFrame | Err:
    """
    Selects a subset of a DataFrame. `key` has some convenience functions

    `key` notes:
    - "*" == all columns
    - "a, b, c" == columns a, b, c (in-order)
    - "a, b : c > 3" == columns a, b where column c > 3
    - "* : c != 3" == all columns where column c != 3

    ... etc.
    """
    _check_assumptions(source)

    # Extract query from key (if present)
    key = key.replace(" ", "")
    query: pl.Expr | None = None
    if ":" in key:
        key, query_str = key.split(":")
        query_str = query_str.strip("[]")
        query = _convert_to_polars_filter(query_str)

    # Extract columns from syntax
    # NOTE: `parsed_col_list` starts with exact user-provided string, then
    #        gets updated in `_generate_nesting_list` to exclude nesting (so matches colname)
    parsed_col_list = re.split(REGEX_COMMA_EXCLUDE_BRACKETS, key)
    nesting_list = _generate_nesting_list(parsed_col_list)

    # Handle "*" case -- replace each instance with `source.columns`
    if "*" in parsed_col_list:
        # Find indices of all occurrences of "*"
        star_idx_list = [i for i, x in enumerate(parsed_col_list) if x == "*"]
        # Replace each "*" with the replacement values
        for idx in reversed(star_idx_list):
            parsed_col_list[idx : idx + 1] = source.columns

    # Grab correct subset/slice of the dataframe
    try:
        res = (
            source.filter(query)[parsed_col_list]
            if isinstance(query, pl.Expr)
            else source[parsed_col_list]
        )
        res = _apply_nesting_list(res, nesting_list, parsed_col_list)
        # Post-processing checks
        if res.is_empty():
            res = default
        # if consume:
        #     # TODO: way to consume just the rows that matched?
        #     for col_name in parsed_col_list:
        #         if col_name in source.columns:
        #             source.drop_in_place(col_name)
    except pl.exceptions.ColumnNotFoundError:
        res = default

    # TODO: Consider supporting regex search and pattern replacements (e.g. prefix_* -> new_prefix_*)
    if rename and isinstance(res, pl.DataFrame):
        res = res.rename(rename)

    return res


def outer_join(
    source: pl.DataFrame, second: pl.DataFrame, on: str | list[str]
) -> pl.DataFrame | Err:
    """
    Applies a left join

    A left join resulting in no change or an empty database results in None
    """
    try:
        _pre_merge_checks(source, second, on)
    except KeyError as e:
        return Err(f"Failed pre-merge checks: {str(e)}")

    res = source.join(second, how="left", on=on, join_nulls=False)

    # If there were no matches, then return `Err`
    #  Check for non-null cols after the left-join
    matched = True
    for col_name in second.columns:
        matched = matched and res.filter(pl.col(col_name).is_not_null()).height > 0
    if not matched:
        return Err("No matching columns on left join")

    # # Only consume if there was a change
    # if consume:
    #     # Only drop rows that were included in the left join
    #     matched_rows = select(res, f"{','.join(on)} ~ [_merge == 'both']")  # type: ignore
    #     # TODO: making assumption on indices here, is this a problem?
    #     # TODO: ^Yes that was a problem, good intuition! Have to match on the _value_
    #     if not matched_rows.is_empty():
    #         second.drop(index=matched_rows.index, inplace=True)  # type: ignore

    return pl.DataFrame(res) if not res.is_empty() else Err("Empty dataframe")


def inner_join(
    source: pl.DataFrame, second: pl.DataFrame, on: str | list[str]
) -> pl.DataFrame | Err:
    """
    Applies an inner join. Returns `None` if nothing was joined
    """
    try:
        _pre_merge_checks(source, second, on)
    except KeyError as e:
        return Err(f"Failed pre-merge checks: {str(e)}")

    res = source.join(second, how="inner", on=on)

    return res if not res.is_empty() else Err("Empty dataframe")


def union(
    source: pl.DataFrame,
    rows=pl.DataFrame | list[dict[str, Any]],
    na_default: Any = None,
    # consume: bool = False,
) -> pl.DataFrame | Err:
    """
    Inserts rows into the end of the DataFrame

    For a row, if a value is not specified it will be filled with the specified default

    If the union operation cannot be done (e.g. incompatible columns), returns `Err`
    """
    if isinstance(rows, list):
        rows = pl.DataFrame(rows)

    # Ensure all columns in `into` are present in `rows`
    for col in source.columns:
        if col not in rows.columns:
            rows = rows.with_columns(pl.lit(na_default).alias(col))

    # Ensure all columns in `rows` are present in `into`
    for col in rows.columns:
        if col not in source.columns:
            source = source.with_columns(pl.lit(na_default).alias(col))

    try:
        res = pl.concat([source, rows])
    except Exception as e:
        return Err(f"Error when unioning: {str(e)}")

    return res


def _check_assumptions(source: pl.DataFrame | Iterable[pl.DataFrame]) -> None:
    if isinstance(source, pl.DataFrame):
        source = (source,)
    for df in source:
        ## Check for column names that are `str`
        col_types = {type(c) for c in df.columns}
        if col_types != {str}:
            raise ValueError(f"Column headers need to be `str`, got: {col_types}")


def _pre_merge_checks(source: pl.DataFrame, second: pl.DataFrame, on: str | list[str]) -> None:
    # If _any_ of the provided indices aren't there, return `None`
    _check_assumptions([source, second])
    if isinstance(on, str):
        on = [on]
    for c in on:
        if not (c in source.columns and c in second.columns):
            raise KeyError(f"Proposed key {c} is not in either column!")


class PythonExprToPolarsExprVisitor(ast.NodeVisitor):
    def visit_BoolOp(self, node):
        if isinstance(node.op, ast.And):
            expr = self.visit(node.values[0])
            for value in node.values[1:]:
                expr = expr & self.visit(value)
        elif isinstance(node.op, ast.Or):
            expr = self.visit(node.values[0])
            for value in node.values[1:]:
                expr = expr | self.visit(value)
        return expr

    def visit_Compare(self, node):
        left = self.visit(node.left)
        comparisons = []
        for op, comparator in zip(node.ops, node.comparators):
            right = self.visit(comparator)
            if isinstance(op, ast.Eq):
                comparisons.append(left == right)
            elif isinstance(op, ast.Gt):
                comparisons.append(left > right)
            elif isinstance(op, ast.Lt):
                comparisons.append(left < right)
            elif isinstance(op, ast.GtE):
                comparisons.append(left >= right)
            elif isinstance(op, ast.LtE):
                comparisons.append(left <= right)
            elif isinstance(op, ast.NotEq):
                comparisons.append(left != right)
        expr = comparisons[0]
        for comparison in comparisons[1:]:
            expr = expr & comparison
        return expr

    def visit_BinOp(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)
        if isinstance(node.op, ast.Add):
            return left + right
        elif isinstance(node.op, ast.Sub):
            return left - right
        elif isinstance(node.op, ast.Mult):
            return left * right
        elif isinstance(node.op, ast.Div):
            return left / right
        elif isinstance(node.op, ast.Mod):
            return left % right

    def visit_Name(self, node):
        return pl.col(node.id)

    def visit_Constant(self, node):
        return pl.lit(node.value)

    def visit(self, node):
        return super().visit(node)


def _convert_to_polars_filter(filter_string: str) -> pl.Expr:
    tree = ast.parse(filter_string, mode="eval")
    visitor = PythonExprToPolarsExprVisitor()
    return visitor.visit(tree.body)


# TODO: NEXT STEP -- this thing
def _apply_nesting_list(
    source: pl.DataFrame,
    nesting_list: list[tuple[bool, str | list[str] | dict[str, str]] | None],
    parsed_col_list: list[str],
) -> pl.DataFrame:
    """
    Completes handling of the `.`, `->`, `+>` operators which is the parsed `nesting_list`

    The incoming "nesting_list" looks something like:
        [None, (True, "b"), (False, "c.d"), None, (False, {"b", "c.d"}) ...]
    """
    # Prevents `SettingWithCopyWarning`, ref: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
    # TODO: does this hog too much extra memory?
    # res = source.clone()
    res = source

    # Apply nesting if applicable
    # NOTE: We keep `(bool, newCol)` instead of just `newCol` to handle indexing of cols correctly
    col_to_del: list[str] = []
    col_to_add: defaultdict[int, list[tuple[bool, pl.Series]]] = defaultdict(list)
    if any(nesting_list):
        for i, nesting in enumerate(nesting_list):
            col_name = parsed_col_list[i]
            if nesting:
                keep_col, col_obj = nesting
                # Single-column case (e.g. "col_name")
                if isinstance(col_obj, str):
                    nesting_expr = _nesting_to_polars_expr(col_obj, col_name)
                    res = res.select(nesting_expr)
                    # HACK: Assume the new column name is the last key in the nesting
                    _, last_part = col_obj.rsplit(".", maxsplit=1)
                    res = res.rename({last_part: f"{col_name}.{col_obj}"})
                # TODO: Next step -- add this back, handle types
                # # Expansion case (`->` or `+>`)
                # elif isinstance(col_obj, list):
                #     for new_nesting in col_obj:
                #         nesting_expr = _nesting_to_polars_expr(new_nesting, col_name)
                #         s: pl.Series = res.select(nesting_expr)
                #         s = s.rename({f"{col_name}.{new_nesting}"})
                #         col_to_add[i].append((keep_col, s))
                #     if not keep_col:
                #         col_to_del.append(res.columns[i])
                # # Expansion case (`->` or `+>`) with renaming
                # elif isinstance(col_obj, dict):
                #     for new_name, new_nesting in col_obj.items():
                #         nesting_expr = _nesting_to_polars_expr(new_nesting, col_name)
                #         s: pl.Series = res.select(nesting_expr)
                #         s = s.rename(new_name)
                #         col_to_add[i].append((keep_col, s))
                #     if not keep_col:
                #         col_to_del.append(res.columns[i])

    # Post-processing clean-up
    if col_to_del:
        res.drop_in_place([col_to_del])  # type: ignore
    if col_to_add:
        bump_idx = 0
        for idx, col_tuple_list in col_to_add.items():
            # FYI: we add +1 if `keep_bool` is True to the new column position (so it goes after)
            for keep_bool, new_col in col_tuple_list:
                res.insert_column(idx + bump_idx + int(keep_bool), new_col)
                bump_idx += 1

    return res


def _nesting_to_polars_expr(nesting: str, col_name: str) -> pl.Expr:
    """
    Converts something like:
        `a.b.c[0].d`
      into:
        `pl.col("a").struct.field("b").struct.field("c").list[0].struct.field("d")`
    """
    res = pl.col(col_name)
    for item in nesting.split("."):
        # Handle brackets -- grab value (assume just integer for now)
        list_idx: str | None = None
        if "[" in item:
            list_idx = item[item.index("[") + 1 : item.index("]")]
        res = res.struct.field(item)
        if list_idx is not None:
            res = res.list[int(list_idx)]
    return res


def _generate_nesting_list(
    parsed_col_list: list[str],
) -> list[tuple[bool, str | list[str] | dict[str, str]] | None]:
    """
    Return whether a specific column index should get nesting logic applied

    Given `parsed_col_list` as follows:
        ['c.d', 'reg_col', 'some_json_col -> ['b', 'c.d']', 'some_json_col +> {'new_name': 'c.d'}]
      The resulting "nesting_list" looks something like:
        [(True, "c.d"), None, (False, ["b", "c.d"]}), (True, {'new_name': 'c.d'})]
      A more complex nesting will index-into different values (set: one -> one/many new cols)
        and possibly rename them as well (dict: one -> one/many new cols with new names)

    For each column, check if:
      1. Column should be extracted and consumed (`->`)
      2. Column should be extracted and kept (`+>`)
      3. Column should be nested into and consumed (using `.` syntax)

    Order matters!

    NOTE: _Not_ a pure function -- assume `parsed_col_list` might be modified
    """
    nesting_list: list[tuple[bool, str | list[str] | dict[str, str]] | None] = []

    for idx, col_name in enumerate(parsed_col_list):
        # 1. extract, and consume original
        # 2. extract, and keep original
        if ("->" in col_name) or ("+>" in col_name):
            keep_col = "+>" in col_name
            splitter = "+>" if keep_col else "->"
            col_name, content = col_name.split(splitter)
            col_obj = _extract_list_or_dict(content)
            if col_obj:
                # NOTE: Remove the nesting from `parsed_col_list` for later processing
                parsed_col_list[idx] = col_name
                nesting_list.append((keep_col, col_obj))
            else:
                nesting_list.append(None)
        # 3. nesting, consume and replace
        elif "." in col_name:
            col_name, nesting = col_name.split(".", maxsplit=1)
            # NOTE: Remove the nesting from `parsed_col_list` for later processing
            parsed_col_list[idx] = col_name
            nesting_list.append((False, nesting))
        else:
            nesting_list.append(None)
    return nesting_list


def _extract_list_or_dict(s: str) -> list[str] | dict[str, str] | None:
    try:
        # Clean string a bit
        s = s.replace(";", "").replace("(", "").replace(")", "")
        res = ast.literal_eval(s)
        if not (isinstance(res, list) or isinstance(res, dict)):
            raise ValueError
        return res
    except (ValueError, SyntaxError):
        return None
