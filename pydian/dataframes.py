import ast
import re
from typing import Any, Callable, Iterable, Literal

import polars as pl
from result import Err

COMMAS_OUTSIDE_OF_BRACKETS = r",(?![^{}\[\]]*[}\]])"
COLON_OUTSIDE_OF_BRACKETS = r":(?![^{]*})"
PERIOD_UP_TO_NEXT_CLOSE_PARENS = r"\.(.*?\))"


def select(
    source: pl.DataFrame,
    key: str,
    consume: bool = False,
    rename: dict[str, str] | Callable[[str], str] | None = None,
) -> pl.DataFrame | Err:
    """
    Selects a subset of a DataFrame. `key` has some convenience functions

    `key` notes:
    - "*" -- all columns
    - "a, b, c" -- columns a, b, c (in-order)
    - "a, b : c > 3" -- columns a, b where column c > 3
    - "* : c != 3" -- all columns where column c != 3
    - "dict_col -> [a, b, c]" -- "dict_col.a, dict_col.b, dict_col.c"
    - "dict_col +> [a, b, c]" -- "dict_col, dict_col -> [a, b, c]"
    - "dict_col -> {"A": a, "B": b}" --"dict_col.a, dict_col.b" and rename `a -> A, b -> B`
    - "dict_col +> {"A": a, "B": b}" -- "dict_col, dict_col -> {"A": a, "B": b}"

    `consume` attempts to drop columns that matched in the `select` from the source dataframe

    `rename` is the standard Polars API call and is called at the very end
    """
    _check_assumptions(source)

    # Extract query from key (if present)
    key = key.replace(" ", "")
    query: pl.Expr | None = None
    if re.search(COLON_OUTSIDE_OF_BRACKETS, key):
        key, query_str = re.split(COLON_OUTSIDE_OF_BRACKETS, key, maxsplit=1)
        query_str = query_str.strip("[]")
        query = _convert_to_polars_filter(query_str)

    # Extract columns from syntax
    parsed_col_list = re.split(COMMAS_OUTSIDE_OF_BRACKETS, key)
    parsed_nested_col_list = _generate_nesting_list(parsed_col_list)

    # Grab correct subset/slice of the dataframe
    try:
        if isinstance(query, pl.Expr):
            source = source.filter(query)
        res = _apply_nested_col_list(source, parsed_nested_col_list)
        # Post-processing checks
        if res.is_empty():
            raise pl.exceptions.ColumnNotFoundError
        if consume:
            # TODO: handle columns with query syntax -- make sure those aren't skipped
            for col_name in parsed_col_list:
                if col_name in source.columns:
                    source.drop_in_place(col_name)
    except pl.exceptions.ColumnNotFoundError:
        return Err("Default Err: `select` key didn't match")

    # TODO: Consider supporting regex search and pattern replacements (e.g. prefix_* -> new_prefix_*)
    if rename and isinstance(res, pl.DataFrame):
        res = res.rename(rename)

    return res


def join(
    source: pl.DataFrame,
    second: pl.DataFrame,
    how: Literal["inner", "left", "cross", "anti", "semi"],
    on: str | list[str],
) -> pl.DataFrame | Err:
    try:
        _pre_merge_checks(source, second, on)
    except KeyError as e:
        return Err(f"Failed pre-merge checks for {how} join: {str(e)}")

    res = source.join(second, how=how, on=on, join_nulls=False, coalesce=True)

    if how == "left":
        # If there were no matches, then return `Err`
        #  Check for non-null cols after the left-join
        matched = True
        for col_name in second.columns:
            matched = matched and res.filter(pl.col(col_name).is_not_null()).height > 0
        if not matched:
            return Err("No matching columns on left join")

    return res if not res.is_empty() else Err("Empty dataframe after left join")


def union(
    source: pl.DataFrame,
    rows=pl.DataFrame | list[dict[str, Any]],
    na_default: Any = None,
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


def group_by(source: pl.DataFrame, agg_str: str, keep_order: bool = True) -> pl.DataFrame | Err:
    """
    Allows the following shorthands for `group_by`:
    - Use comma-delimited col names
    - Specify aggregators after `->` using list or dict syntax
        - For no aggregator specified, default to `.all()`

    Examples:
    - `"a"` -- `group_by('a').all()`
    - `"a, b"` -- `group_by(['a', 'b']).all()`
    - `"a -> ['*'.len()]"` -- `group_by('a').len()`
    - `"a -> ['b'.mean()]"` -- `group_by('a').agg([pl.col('b').mean()]))
    - `"a -> ['*'.mean()]"` -- `group_by('a').mean()`

    Supported aggregation functions:
    - `len()`
    - `sum()`, `mean()`
    - `max()`, `min()`, `median()`
    - `std()`, `var()`
    """
    # Parse `agg` str into halfs
    agg_str = agg_str.replace(" ", "")
    query_parts = agg_str.split("->")
    res = source
    if len(query_parts) == 1:
        # Default to using `.all()` aggregator
        col_names = agg_str.split(",")
        res = source.group_by(col_names, maintain_order=keep_order).all()
    elif len(query_parts) == 2:
        col_names = query_parts[0].split(",")
        res = source.group_by(col_names, maintain_order=keep_order)  # type: ignore
        agg_list_str = query_parts[1].removeprefix("[").removesuffix("]")
        agg_expr_list = _agg_list_to_polars_expr(agg_list_str)
        if isinstance(agg_expr_list, Err):
            return agg_expr_list
        else:
            res = res.agg(agg_expr_list)  # type: ignore
    else:
        raise ValueError("Groupby aggregation string contained too many `->` characters")

    if res.is_empty():
        return Err("Dataframe after `group_by` is empty")

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


def _apply_nested_col_list(
    source: pl.DataFrame,
    parsed_nested_col_list: list[str | tuple[bool, list[str] | dict[str, str]]],
) -> pl.DataFrame:
    """
    Completes handling of the `.`, `->`, `+>` operators which is the `parsed_nested_col_list`.
      Converts the string expressions into corresponding Polars expression to apply at the end

    The incoming "parsed_nested_col_list" looks something like:
        ["some_col", "b", "c.d", (True, ["b", "c.d"]), (False, {"new_name": "c.d"}) ...]
    """

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
    for i, nested_col in enumerate(parsed_nested_col_list):
        # Single-column case (e.g. "col_name")
        if isinstance(nested_col, str):
            nesting_expr = _nesting_to_polars_expr(nested_col)
            expr_list.append(nesting_expr)
        # # Expansion case (`->` or `+>`)
        elif isinstance(nested_col, tuple):
            keep_col, col_obj = nested_col
            # keep column if specified
            if keep_col:
                expr_list.append(pl.col(res.columns[i]))
            if isinstance(col_obj, list):
                for new_nesting in col_obj:
                    nesting_expr = _nesting_to_polars_expr(new_nesting)
                    expr_list.append(nesting_expr)
            # renaming case
            elif isinstance(col_obj, dict):
                for new_name, new_nesting in col_obj.items():
                    nesting_expr = _nesting_to_polars_expr(new_nesting, new_name)
                    expr_list.append(nesting_expr)
    if expr_list:
        res = res.select(expr_list)
    return res


def _nesting_to_polars_expr(nested_col: str, new_name: str | None = None) -> pl.Expr:
    """
    Converts something like:
        `a.b.c[0].d`
      into:
        `pl.col("a").struct.field("b").struct.field("c").list[0].struct.field("d")`
    """
    nesting_list = nested_col.split(".", maxsplit=1)

    res = pl.col(nesting_list[0])
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
        res = res.alias(nested_col)

    return res


def _generate_nesting_list(
    parsed_col_list: list[str],
) -> list[str | tuple[bool, list[str] | dict[str, str]]]:
    """
    Return whether a specific column index should get nesting logic applied

    Given `parsed_col_list` as follows:
        ['c.d', 'reg_col', "some_json_col -> ['b', 'c.d']", "some_json_col +> {'new_name': 'c.d'}"]
      The resulting "parsed_nested_col_list" looks something like -- a tuple of `(keep_col, nesting)`:
        ['c.d', 'reg_col', (False, ['some_json_col.b', 'some_json_col.c.d']}), (True, {'new_name': 'some_json_col.c.d'})]
      A more complex nesting will index-into different values (set: one -> one/many new cols)
        and possibly rename them as well (dict: one -> one/many new cols with new names)

    For each column, check if:
      1. Column should be extracted and consumed (`->`)
      2. Column should be extracted and kept (`+>`)
      3. Column should be nested into and consumed (any other str and supporting `.` syntax)
    """
    parsed_nested_col_list: list[str | tuple[bool, list[str] | dict[str, str]]] = []

    for col_name in parsed_col_list:
        # 1. extract, and consume original
        # 2. extract, and keep original
        if ("->" in col_name) or ("+>" in col_name):
            keep_col = "+>" in col_name
            col_name, content = col_name.split("+>" if keep_col else "->")
            col_obj = _extract_list_or_dict(content, add_prefix=col_name)
            if col_obj:
                parsed_nested_col_list.append((keep_col, col_obj))
            else:
                raise RuntimeError("Error processing `->` or `+>` syntax")
        # 3. regular string nesting
        else:
            parsed_nested_col_list.append(col_name)
    return parsed_nested_col_list


def _extract_list_or_dict(
    s: str, add_prefix: str | None = None
) -> list[str] | dict[str, str] | None:
    """
    Tries to convert a string into a list[str] or dict[str, str]

    If `add_prefix` is specified, then adds the prefix to list/dict values (not keys)
    """
    try:
        # Clean string a bit
        s = s.replace(";", "").replace("(", "<fn").replace(")", ">")
        res = ast.literal_eval(s)
        if not (isinstance(res, list) or isinstance(res, dict)):
            raise ValueError("Need to specify a list or dict after `->` or `+>`")
        # Add a prefix to list/dict values if specified
        if add_prefix and isinstance(res, list):
            res = [f"{add_prefix}.{s}" for s in res]
        elif add_prefix and isinstance(res, dict):
            res = {k: f"{add_prefix}.{v}" for k, v in res.items()}
        return res
    except (ValueError, SyntaxError):
        return None


def _agg_list_to_polars_expr(agg_list_str: str) -> list[pl.Expr] | Err:
    """
    Takes something like:
        - "a -> ['*'.len()]"
        - "a -> ['b'.mean()]"
        - "a -> ['b'.mean(), 'c'.median()]
      and turns it into:
        - [pl.col('*').len()]
        - [pl.col('b').mean()]
        - [pl.col('b').mean(), pl.col('c').median()]

    Supported aggregation functions:
    - `len()`
    - `sum()`, `mean()`
    - `max()`, `min()`, `median()`
    - `std()`, `var()`
    """

    # Split into cols and aggregators
    # First split by commas to get each individual part
    # For each part:
    #  - Assume if `()` is present, it's an aggregator for the last noted col (in quotes)
    #  - Assume the first item must be a col name, and the first character must be a quote
    agg_cols = agg_list_str.split(",")
    agg_parts = []
    for col in agg_cols:
        col_parts = re.split(PERIOD_UP_TO_NEXT_CLOSE_PARENS, col)
        agg_parts.extend(col_parts)
    quote = agg_parts[0][0]
    if quote not in {'"', "'"}:
        return Err(f"Need to have column expressions in quotes, got `{agg_parts[0]}`")
    lexed_agg_list_str = str([p.strip(quote) for p in agg_parts if p != ""])
    parsed_agg_list_str = _extract_list_or_dict(
        lexed_agg_list_str
    )  # NOTE: this fn replaces `()` from the string with `<fn>`
    if parsed_agg_list_str is None:
        return Err(f"Could not parse expression `{agg_list_str}`")

    # Apply the appropriate aggregation function
    res = []
    curr_expr: pl.Expr | None = None
    for agg_part in parsed_agg_list_str:
        if not agg_part.endswith("<fn>"):
            # Assume default `all()` if nothing specified
            if curr_expr is not None:
                res.append(curr_expr.all())
            # Set-up next aggregator
            curr_expr = pl.col(agg_part)
        else:
            # NOTE: each of these aggregators will be considered "terminal". No chaining currently
            #   ... also no logic-checking / correcting. Just handling as-is!
            if not isinstance(curr_expr, pl.Expr):
                return Err(
                    f"Error when handling aggregation expression, tried to aggregate incorrect type: {curr_expr}"
                )
            match agg_part:
                case "len<fn>":
                    curr_expr = curr_expr.len()
                case "sum<fn>":
                    curr_expr = curr_expr.sum()
                case "mean<fn>":
                    curr_expr = curr_expr.mean()
                case "max<fn>":
                    curr_expr = curr_expr.max()
                case "min<fn>":
                    curr_expr = curr_expr.min()
                case "median<fn>":
                    curr_expr = curr_expr.median()
                case "std<fn>":
                    curr_expr = curr_expr.std()
                case "var<fn>":
                    curr_expr = curr_expr.var()  # .alias(f"{curr_expr.meta.output_name()}_sum")
                case _:
                    return Err(f"Got unsupported aggregator: {agg_part}")
            # Rename column based on the aggregator
            #   Handle the `*` case by checking for root name
            agg_suffix = agg_part.removesuffix("<fn>")
            if root_names := curr_expr.meta.root_names():
                curr_expr = curr_expr.alias(f"{root_names[0]}_{agg_suffix}")
            else:
                # The `*` case -- add a suffix
                curr_expr = curr_expr.name.suffix(f"_{agg_suffix}")
            res.append(curr_expr)
            curr_expr = None

    # Handle last item case (e.g. `b` for "a, b")
    if curr_expr is not None:
        res.append(curr_expr.all())

    return res
