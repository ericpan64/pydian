import ast
import re
from typing import Any, Callable, Iterable, Literal

import polars as pl
from result import Err

COMMAS_IGNORING_BRACKETS = r",(?![^{}\[\]]*[}\]])"
COLONS_IGNORING_BRACES = r":(?![^{]*})"
PERIOD_UP_TO_NEXT_CLOSE_PARENS = r"\.(.*?\))"

JOIN_KEYWORD = re.compile(r"\bFROM\b", re.IGNORECASE)
ON_KEYWORD = re.compile(r"\bON\b", re.IGNORECASE)
ON_COLS_PATTERN = r"\bon\s*\[(.*?)\]"  # TODO: refactor this at some point, it's a hack

# Alright. Only support up to 26 tables max at a time. That's it. No exceptions! \s
TABLE_ALIASES = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def select(
    source: pl.DataFrame,
    key: str,
    others: pl.DataFrame | list[pl.DataFrame] | None = None,
    rename: dict[str, str] | Callable[[str], str] | None = None,
) -> pl.DataFrame | Err:
    """
    Selects a subset of a DataFrame. `key` has some convenience functions

    `key` notes:
    - query syntax:
        - "*" -- all columns
        - "a, b, c" -- columns a, b, c (in-order)
        - "a, b : c > 3" -- columns a, b where column c > 3
        - "* : c != 3" -- all columns where column c != 3
        - "dict_col -> [a, b, c]" -- "dict_col.a, dict_col.b, dict_col.c"
    NOTE: For the rest of these operations, only _one_ of each kind is currently supported
    NOTE: By default, the pydian DSL uses `A` as an alias for `source`,
          and `B`, `C`, etc. (up to `Z`) for corresponding dataframes in `others`
    - join synytax:
        - "a, b from A <- B on [col_name]" -- outer left join onto `col_name`
        - "* from A <> B on [col_name]" -- inner join on `col_name`
    # TODO: add union syntax (++)
    # TODO: add group_by syntax (brainstorm? Maybe like: `(A | group[col_name] -> [sum(), max()])`.. etc.)
    # TODO: decide on how to do subqueries and whatnot. Probably after figuring out better parsing strategy
    #       (will need to do that with `get` too -- CFG time? Probably!)
    # So: currently only supports one join (do a CFG to properly support multiple)

    `rename` is the standard Polars API call and is called at the very end
    """

    # `join` logic (apply if applicable)
    # Identify if `join` logic applies
    if re.search(JOIN_KEYWORD, key):
        key, join_clause = re.split(JOIN_KEYWORD, key, maxsplit=1)
        source = _try_join(join_clause, source, others)  # type: ignore
        if isinstance(source, Err):
            return source

    # Extract `:`-based query syntax from key (if present)
    key = key.replace(" ", "")  # Remove whitespace
    query: pl.Expr | None = None
    if re.search(COLONS_IGNORING_BRACES, key):
        key, query_str = re.split(COLONS_IGNORING_BRACES, key, maxsplit=1)
        query_str = query_str.strip("[]")
        query = _convert_to_polars_filter(query_str)
    ## Filter if the query is used
    if isinstance(query, pl.Expr):
        source = source.filter(query)

    # Main `query` logic (columns and ., ->)
    try:
        # Grab correct subset/slice of the dataframe
        parsed_col_list = re.split(
            COMMAS_IGNORING_BRACKETS, key
        )  # Get distinct space for each column name
        res = _apply_nested_col_list(source, parsed_col_list)
        # Post-processing checks
        if res.is_empty():
            raise pl.exceptions.ColumnNotFoundError
    except pl.exceptions.ColumnNotFoundError:
        return Err("<Default Err> `select` key didn't match anything (ColumnNotFoundError)")

    # TODO: Consider supporting regex search and pattern replacements (e.g. prefix_* -> new_prefix_*)
    if rename and isinstance(res, pl.DataFrame):
        res = res.rename(rename)

    return res


def _try_join(
    join_clause: str,
    source: pl.DataFrame,
    others: pl.DataFrame | list[pl.DataFrame] | None = None,
) -> pl.DataFrame | Err:
    """
    Attempts to do `join` based on the provided key

    NOTE: This just does one join for now. So no nested nonsense (yet)
    """
    how = "left" if "<-" in join_clause else "inner" if "<>" in join_clause else None
    if not isinstance(others, list):
        others = [others]  # type: ignore
    # join_alias_names = list(TABLE_ALIASES[:len(others) + 2])
    # HACK: Alright. Just do the join on one thing for now. Fix this with a CFG implementation.
    if match := re.search(ON_COLS_PATTERN, join_clause, re.IGNORECASE):
        on = [col.strip() for col in match.group(1).split(",")]
    else:
        return Err("No join columns specified in brackets after 'on'")

    # Alright. Actually do the join
    second = others[0]
    try:
        # If _any_ of the provided indices aren't there, return `Err`
        if isinstance(on, str):
            on = [on]
        for c in on:
            if not (c in source.columns and c in second.columns):
                raise KeyError(f"Proposed key {c} is not in either column!")
    except KeyError as e:
        return Err(f"Failed pre-merge checks for {how} join: {str(e)}")

    res = source.join(second, how=how, on=on, join_nulls=False, coalesce=True)  # type: ignore

    # NOTE: checking if left join didn't match anything (can't just do empty check bc it's outer join)
    if how == "left":
        # If there were no matches, then return `Err`
        #  Check for non-null cols after the left-join
        matched = True
        for col_name in second.columns:
            matched = matched and res.filter(pl.col(col_name).is_not_null()).height > 0
        if not matched:
            return Err("No matching columns on left join")

    return res if not res.is_empty() else Err("Empty dataframe after join")


# def union(
#     source: pl.DataFrame,
#     rows=pl.DataFrame | list[dict[str, Any]],
#     na_default: Any = None,
# ) -> pl.DataFrame | Err:
#     """
#     Inserts rows into the end of the DataFrame

#     For a row, if a value is not specified it will be filled with the specified default

#     If the union operation cannot be done (e.g. incompatible columns), returns `Err`
#     """
#     if isinstance(rows, list):
#         rows = pl.DataFrame(rows)

#     # Ensure all columns in `into` are present in `rows`
#     for col in source.columns:
#         if col not in rows.columns:
#             rows = rows.with_columns(pl.lit(na_default).alias(col))

#     # Ensure all columns in `rows` are present in `into`
#     for col in rows.columns:
#         if col not in source.columns:
#             source = source.with_columns(pl.lit(na_default).alias(col))

#     try:
#         res = pl.concat([source, rows])
#     except Exception as e:
#         return Err(f"Error when unioning: {str(e)}")

#     return res


# def group_by(source: pl.DataFrame, agg_str: str, keep_order: bool = True) -> pl.DataFrame | Err:
#     """
#     Allows the following shorthands for `group_by`:
#     - Use comma-delimited col names
#     - Specify aggregators after `->` using list or dict syntax
#         - For no aggregator specified, default to `.all()`

#     Examples:
#     - `"a"` -- `group_by('a').all()`
#     - `"a, b"` -- `group_by(['a', 'b']).all()`
#     - `"a -> ['*'.len()]"` -- `group_by('a').len()`
#     - `"a -> ['b'.mean()]"` -- `group_by('a').agg([pl.col('b').mean()]))
#     - `"a -> ['*'.mean()]"` -- `group_by('a').mean()`

#     Supported aggregation functions:
#     - `len()`
#     - `sum()`, `mean()`
#     - `max()`, `min()`, `median()`
#     - `std()`, `var()`
#     """
#     # Parse `agg` str into halfs
#     agg_str = agg_str.replace(" ", "")
#     query_parts = agg_str.split("->")
#     res = source
#     if len(query_parts) == 1:
#         # Default to using `.all()` aggregator
#         col_names = agg_str.split(",")
#         res = source.group_by(col_names, maintain_order=keep_order).all()
#     elif len(query_parts) == 2:
#         col_names = query_parts[0].split(",")
#         res = source.group_by(col_names, maintain_order=keep_order)  # type: ignore
#         agg_list_str = query_parts[1].removeprefix("[").removesuffix("]")
#         agg_expr_list = _agg_list_to_polars_expr(agg_list_str)
#         if isinstance(agg_expr_list, Err):
#             return agg_expr_list
#         else:
#             res = res.agg(agg_expr_list)  # type: ignore
#     else:
#         raise ValueError("Groupby aggregation string contained too many `->` characters")

#     if res.is_empty():
#         return Err("Dataframe after `group_by` is empty")

#     return res


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


def _apply_nested_col_list(
    source: pl.DataFrame,
    parsed_col_list: list[str],
) -> pl.DataFrame:
    """
    Completes handling of the `.`, `->` operators which is the `parsed_nested_col_list`.
      Converts the string expressions into corresponding Polars expression to apply at the end

    The incoming "parsed_nested_col_list" looks something like:
        ["some_col", "b", "c.d", ["b", "c.d"], {"new_name": "c.d"} ...]
    """
    # Handle `->` case
    parsed_nested_col_list = _generate_nesting_list(parsed_col_list)

    # Handle "*" case -- replace each instance with `source.columns`
    if "*" in parsed_nested_col_list:
        # Find indices of all occurrences of "*"
        star_idx_list = [i for i, x in enumerate(parsed_nested_col_list) if x == "*"]
        # Replace each "*" with the replacement values
        for idx in reversed(star_idx_list):
            parsed_nested_col_list[idx : idx + 1] = source.columns

    # For each column specified, convert it to the corresponding Polars expression.
    #   Apply the expression at the end to get the final result
    res = source
    expr_list = []
    for col_str in parsed_nested_col_list:
        # Single-column case (e.g. "col_name")
        if isinstance(col_str, str):
            nesting_expr = _colname_to_polars_expr(col_str)
            expr_list.append(nesting_expr)
        # # Expansion case (`->`)
        else:
            col_obj = col_str
            # base case
            if isinstance(col_obj, list):
                for colname in col_obj:
                    nesting_expr = _colname_to_polars_expr(colname)
                    expr_list.append(nesting_expr)
            # # renaming case
            # elif isinstance(col_obj, dict):
            #     for colname, new_name in col_obj.items():
            #         nesting_expr = _colname_to_polars_expr(colname, new_name)
            #         expr_list.append(nesting_expr)
            else:
                raise RuntimeError(
                    f"Got unexpected type in `_apply_nested_col_list`: {type(col_str)}"
                )
    # Finally apply the `select`
    if expr_list:
        res = res.select(expr_list)
    return res


def _colname_to_polars_expr(col_str: str, new_name: str | None = None) -> pl.Expr:
    """
    Converts something like:
        `a.b.c[0].d`
      into:
        `pl.col("a").struct.field("b").struct.field("c").list[0].struct.field("d")`
    """
    nesting_list = col_str.split(".", maxsplit=1)

    res = pl.col(nesting_list[0])

    # TODO: refactor this with regex
    if len(nesting_list) > 1:
        for item in nesting_list[1].split("."):
            # Handle brackets -- grab value (assume just integer for now)
            list_idx: str | None = None
            if "[" in item:
                lbracket_idx = item.index("[")
                list_idx = item[lbracket_idx + 1 : item.index("]")]
                item = item[:lbracket_idx]  # Remove bracket from original str
            res = res.struct.field(item)
            if list_idx is not None:
                # TODO: Handle more than just single index, e.g. handle slices?
                res = res.list[int(list_idx)]

    if new_name:
        res = res.alias(new_name)
    else:
        res = res.alias(col_str)

    return res


def _generate_nesting_list(
    parsed_col_list: list[str],
) -> list[str | list[str] | dict[str, str]]:
    """
    Return whether a specific column index should get nesting logic applied

    Given `parsed_col_list` as follows:
        ['c.d', 'reg_col', "some_json_col -> ['b', 'c.d']", "some_json_col -> {'new_name': 'c.d'}"]
      The resulting "parsed_nested_col_list" looks something like -- a tuple of `(keep_col, nesting)`:
        ['c.d', 'reg_col', ['some_json_col.b', 'some_json_col.c.d'], {'new_name': 'some_json_col.c.d'}]

      A more complex nesting will index-into different values (set: one -> one/many new cols)
        and possibly rename them as well (dict: one -> one/many new cols with new names)

    For each column, check if:
      1. Column should be extracted and consumed (`->`)
      2. Column should be nested into and consumed (any other str and supporting `.` syntax)
    """
    parsed_nested_col_list: list[str | list[str] | dict[str, str]] = []

    for col_name in parsed_col_list:
        # 1. extract, and keep original
        if "->" in col_name:
            col_name, content = col_name.split("->")
            col_obj = _extract_list(content, add_prefix=col_name)
            if col_obj:
                parsed_nested_col_list.append(col_obj)
            else:
                raise RuntimeError("Error processing `->` syntax")
        # 2. regular string as-is (nested case handled implicitly)
        else:
            parsed_nested_col_list.append(col_name)
    return parsed_nested_col_list


def _extract_list(s: str, add_prefix: str | None = None) -> list[str] | None:
    """
    Converts a string representation into a list[str].
    Handles unwrapped strings in list format, e.g.:
        [a, b, c] -> ['a', 'b', 'c']

    If `add_prefix` is specified, adds the prefix to each value:
        [a, b, c] with prefix "x" -> ['x.a', 'x.b', 'x.c']
    """
    try:
        # Validate list format
        if not (s.startswith("[") and s.endswith("]")):
            raise ValueError("Input must be wrapped in []")

        # Extract and clean items
        content = s[1:-1]  # Remove outer brackets
        items = [item.strip() for item in content.split(",") if item.strip()]  # Skip empty items

        # Add prefix if specified
        if add_prefix:
            items = [f"{add_prefix}.{item}" for item in items]

        return items

    except ValueError:
        return None


# def _agg_list_to_polars_expr(agg_list_str: str) -> list[pl.Expr] | Err:
#     """
#     Takes something like:
#         - "a -> ['*'.len()]"
#         - "a -> ['b'.mean()]"
#         - "a -> ['b'.mean(), 'c'.median()]
#       and turns it into:
#         - [pl.col('*').len()]
#         - [pl.col('b').mean()]
#         - [pl.col('b').mean(), pl.col('c').median()]

#     Supported aggregation functions:
#     - `len()`
#     - `sum()`, `mean()`
#     - `max()`, `min()`, `median()`
#     - `std()`, `var()`
#     """

#     # Split into cols and aggregators
#     # First split by commas to get each individual part
#     # For each part:
#     #  - Assume if `()` is present, it's an aggregator for the last noted col (in quotes)
#     #  - Assume the first item must be a col name, and the first character must be a quote
#     agg_cols = agg_list_str.split(",")
#     agg_parts = []
#     for col in agg_cols:
#         col_parts = re.split(PERIOD_UP_TO_NEXT_CLOSE_PARENS, col)
#         agg_parts.extend(col_parts)
#     quote = agg_parts[0][0]
#     if quote not in {'"', "'"}:
#         return Err(f"Need to have column expressions in quotes, got `{agg_parts[0]}`")
#     lexed_agg_list_str = str([p.strip(quote) for p in agg_parts if p != ""])
#     parsed_agg_list_str = _extract_list(
#         lexed_agg_list_str
#     )  # NOTE: this fn replaces `()` from the string with `<fn>`
#     if parsed_agg_list_str is None:
#         return Err(f"Could not parse expression `{agg_list_str}`")

#     # Apply the appropriate aggregation function
#     res = []
#     curr_expr: pl.Expr | None = None
#     for agg_part in parsed_agg_list_str:
#         if not agg_part.endswith("<fn>"):
#             # Assume default `all()` if nothing specified
#             if curr_expr is not None:
#                 res.append(curr_expr.all())
#             # Set-up next aggregator
#             curr_expr = pl.col(agg_part)
#         else:
#             # NOTE: each of these aggregators will be considered "terminal". No chaining currently
#             #   ... also no logic-checking / correcting. Just handling as-is!
#             if not isinstance(curr_expr, pl.Expr):
#                 return Err(
#                     f"Error when handling aggregation expression, tried to aggregate incorrect type: {curr_expr}"
#                 )
#             match agg_part:
#                 case "len<fn>":
#                     curr_expr = curr_expr.len()
#                 case "sum<fn>":
#                     curr_expr = curr_expr.sum()
#                 case "mean<fn>":
#                     curr_expr = curr_expr.mean()
#                 case "max<fn>":
#                     curr_expr = curr_expr.max()
#                 case "min<fn>":
#                     curr_expr = curr_expr.min()
#                 case "median<fn>":
#                     curr_expr = curr_expr.median()
#                 case "std<fn>":
#                     curr_expr = curr_expr.std()
#                 case "var<fn>":
#                     curr_expr = curr_expr.var()  # .alias(f"{curr_expr.meta.output_name()}_sum")
#                 case _:
#                     return Err(f"Got unsupported aggregator: {agg_part}")
#             # Rename column based on the aggregator
#             #   Handle the `*` case by checking for root name
#             agg_suffix = agg_part.removesuffix("<fn>")
#             if root_names := curr_expr.meta.root_names():
#                 curr_expr = curr_expr.alias(f"{root_names[0]}_{agg_suffix}")
#             else:
#                 # The `*` case -- add a suffix
#                 curr_expr = curr_expr.name.suffix(f"_{agg_suffix}")
#             res.append(curr_expr)
#             curr_expr = None

#     # Handle last item case (e.g. `b` for "a, b")
#     if curr_expr is not None:
#         res.append(curr_expr.all())

#     return res
