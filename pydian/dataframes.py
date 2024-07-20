import ast
import re
from typing import Any, Iterable

import polars as pl
from result import Err

import pydian.partials as p

from .lib.types import ApplyFunc, ConditionalCheck

REGEX_COMMA_EXCLUDE_BRACKETS = r",(?![^{}]*\})"


def select(
    source: pl.DataFrame,
    key: str,
    default: Any = Err("Default Err: key didn't match"),
    consume: bool = False,
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
    res = _nested_select(source, key, default, consume)
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
    for cname in second.columns:
        matched = matched and res.filter(pl.col(cname).is_not_null()).height > 0
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


def _pre_merge_checks(source: pl.DataFrame, second: pl.DataFrame, on: str | list[str]) -> None:
    # If _any_ of the provided indices aren't there, return `None`
    _check_assumptions([source, second])
    if isinstance(on, str):
        on = [on]
    for c in on:
        if not (c in source.columns and c in second.columns):
            raise KeyError(f"Proposed key {c} is not in either column!")


def _nested_select(
    source: pl.DataFrame, key: str, default: Any, consume: bool
) -> pl.DataFrame | Any:
    res = None

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
    # nesting_list = _generate_nesting_list(parsed_col_list)

    # Handle "*" case
    # TODO: Handle "*" with other items, e.g. `"*, a -> {b, c}`?
    if parsed_col_list == ["*"]:
        parsed_col_list = source.columns

    try:
        res = (
            source.filter(query)[parsed_col_list]
            if isinstance(query, pl.Expr)
            else source[parsed_col_list]
        )
        # res = _apply_nesting_list(res, nesting_list, parsed_col_list)
        # Post-processing checks
        if res.is_empty():
            res = default
        # if consume:
        #     # TODO: way to consume just the rows that matched?
        #     for cname in parsed_col_list:
        #         if cname in source.columns:
        #             source.drop_in_place(cname)
    except pl.exceptions.ColumnNotFoundError:
        res = default

    return res


class PolarsExpressionVisitor(ast.NodeVisitor):
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
    visitor = PolarsExpressionVisitor()
    return visitor.visit(tree.body)


# def _apply_nesting_list(
#     source: pl.DataFrame,
#     nesting_list: list[str | tuple[bool, list[str] | dict[str, str]] | None],
#     parsed_col_list: list[str],
# ) -> pl.DataFrame:
#     # Prevents `SettingWithCopyWarning`, ref: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
#     # TODO: does this hog too much extra memory?
#     res = source.clone()

#     # Apply nesting if applicable
#     col_idx_to_del: list[int] = []
#     col_to_add: defaultdict[int, list[tuple[bool, pl.Series]]] = defaultdict(list)
#     if any(nesting_list):
#         for i, nesting in enumerate(nesting_list):  # type: ignore
#             cname = parsed_col_list[i]
#             match nesting:
#                 case str():
#                     s: pl.Series = res[cname].apply(p.get(nesting))
#                     s.name = f"{cname}.{nesting}"
#                     res = alter(res, overwrite_cols={cname: s})
#                 case tuple():
#                     keep_col, cobj = nesting
#                     if not keep_col:
#                         col_idx_to_del.append(i)
#                     if isinstance(cobj, list):
#                         for nkey in cobj:
#                             s: pl.Series = res.loc[:, cname].apply(p.get(nkey))  # type: ignore
#                             s.name = f"{cname}.{nkey}"
#                             col_to_add[i].append((keep_col, s))
#                     elif isinstance(cobj, dict):
#                         for new_name, nkey in cobj.items():
#                             s: pl.Series = res.loc[:, cname].apply(p.get(nkey))  # type: ignore
#                             s.name = new_name
#                             col_to_add[i].append((keep_col, s))
#                 case None:
#                     continue

#     # Do `tuple` case procecssing
#     if col_idx_to_del:
#         res.drop(res.columns[col_idx_to_del])
#     if col_to_add:
#         bump_idx = 0
#         for idx, vlist in col_to_add.items():
#             # FYI: unions at the front, so add extra 1 if we kept the original column
#             for kcbool, s in vlist:
#                 res.union(idx + bump_idx + int(kcbool), s.name, s.values)
#                 bump_idx += 1

#     return res


# def _generate_nesting_list(
#     parsed_col_list: list[str],
# ) -> list[str | tuple[bool, list[str] | dict[str, str]] | None]:
#     """
#     Return whether a specific column index should get nesting logic applied

#     For each column, check if:
#       1. Column should be extracted and consumed (`->`)
#       2. Column should be extracted and kept (`+>`)
#       3. Column should be nested into and consumed (exactly once)

#     Order matters!

#     Not a pure function -- assume `parsed_col_list` might be modified
#     """
#     nesting_list: list[str | tuple[bool, list[str] | dict[str, str]] | None] = []
#     # for i, c in enumerate(parsed_col_list):
#     for i, c in enumerate(parsed_col_list):
#         # 1. extract, and consume original
#         # 2. extract, and keep original
#         if ("->" in c) or ("+>" in c):
#             keep_col = "+>" in c
#             splitter = "+>" if keep_col else "->"
#             cname, content = c.split(splitter)
#             cobj = _extract_list_or_dict(content)
#             if cobj:
#                 # NOTE: Remove the nesting from `parsed_col_list` for later processing
#                 parsed_col_list[i] = cname
#                 nesting_list.append((keep_col, cobj))
#             else:
#                 nesting_list.append(None)
#         # 3. nesting, consume and replace
#         elif "." in c:
#             cname, nesting = c.split(".", maxsplit=1)
#             # NOTE: Remove the nesting from `parsed_col_list` for later processing
#             parsed_col_list[i] = cname
#             nesting_list.append(nesting)
#         else:
#             nesting_list.append(None)
#     return nesting_list


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
