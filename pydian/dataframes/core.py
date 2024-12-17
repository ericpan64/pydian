import ast
import re
from typing import Any, Callable, Iterable, Literal

import polars as pl
from result import Err

# `Bracket` is `[]`, `Braces` is `{}`
COMMAS_IGNORING_BRACKETS_BRACES = r",(?![^{}\[\]]*[}\]])"
COLONS_IGNORING_BRACES = r":(?![^{]*})"
PERIOD_UP_TO_NEXT_CLOSE_PARENS = r"\.(.*?\))"
STR_WITHIN_BRACKETS = r"\[([^\]]+)\]"

FROM_KEYWORD = re.compile(r"\bFROM\b", re.IGNORECASE)
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
        - "a, b : [c > 3]" -- columns a, b where column c > 3
        - "* : [c != 3]" -- all columns where column c != 3
        - "dict_col -> [a, b, c]" -- "dict_col.a, dict_col.b, dict_col.c"
    NOTE: For the rest of these operations, only _one_ of each kind is currently supported
    NOTE: By default, the pydian DSL uses `A` as an alias for `source`,
          and `B`, `C`, etc. (up to `Z`) for corresponding dataframes in `others`
    - join synytax:
        - "a, b from A <- B on [col_name]" -- outer left join onto `col_name`
        - "* from A <> B on [col_name]" -- inner join on `col_name`
        - "* from A ++ B" -- append B into A (whatever columns match)
        - "* from A => groupby[col_name | sum(), max()]"
    - groupby synax:
        - "col_name, other_col from A => groupby[col_name]"
        - "col_name, other_col_sum from A => groupby[col_name | sum()]"
        - "* from A => groupby[col_name, other_col | n_unique(), sum()]
    # TODO: decide on how to do subqueries and whatnot. Probably after figuring out better parsing strategy
    #       (will need to do that with `get` too -- CFG time? Probably!)
    # So: currently only supports one join (do a CFG to properly support multiple)

    `rename` is the standard Polars API call and is called at the very end
    """

    # `from` logic (apply if applicable)
    # Identify if `join`, `union`, or `groupby` logic applies
    if re.search(FROM_KEYWORD, key):
        key, clause = re.split(FROM_KEYWORD, key, maxsplit=1)
        # TODO: This only allows 1 operation per query, figure out how to do multiple
        if "++" in clause:
            source = _try_union(clause, source, others)  # type: ignore
        elif " on " in clause.lower():
            source = _try_join(clause, source, others)  # type: ignore
        elif "=>" in clause:
            # TODO: would be cool to also support `orderby` here too
            source = _try_groupby(clause, source, others)  # type: ignore
        else:
            raise RuntimeError("Error: missing/unsupported operation in `from` clause")
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
            COMMAS_IGNORING_BRACKETS_BRACES, key
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


def _try_union(
    merge_clause: str,
    source: pl.DataFrame,
    other=pl.DataFrame,
    na_default: Any = None,
) -> pl.DataFrame | Err:
    """
    Inserts rows into the end of the DataFrame

    For a row, if a value is not specified it will be filled with the specified default

    If the union operation cannot be done (e.g. incompatible columns), returns `Err`
    """
    # HACK: Going to revisit this with CFG parsing. For now, just assume it's just "A ++ B"
    rows = other

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
        "len()": pl.len().name.suffix("_len"),
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
