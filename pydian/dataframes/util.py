import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal, Sequence, TypeAlias

import polars as pl
from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from ..dicts.core import get
from ..lib.types import KEEP
from ..lib.util import flatten_sequence

COLNAMES_DSL_GRAMMAR = Grammar(Path(__file__).parent.joinpath("dsl/colnames.peg").read_text())
FROM_DSL_GRAMMAR = Grammar(Path(__file__).parent.joinpath("dsl/from.peg").read_text())
GROUP_DSL_GRAMMAR = Grammar(Path(__file__).parent.joinpath("dsl/group.peg").read_text())

# fmt: off
TableAlias: TypeAlias = Literal["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",   
                                "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"]
# fmt: on

@dataclass(frozen=True)
class FromExpr:
    lhs: TableAlias
    rhs: TableAlias

@dataclass(frozen=True)
class JoinExpr(FromExpr):
    join_type: Literal["LEFT", "INNER"]
    on_cols: list[str]


@dataclass(frozen=True)
class UnionExpr(FromExpr):
    pass

@dataclass(frozen=True)
class GroupExpr:
    op_type: Literal["GROUPBY", "ORDERBY"]
    on_cols: list[str]
    agg_fns: list[pl.Expr]


SelectDslTreeResult: TypeAlias = tuple[list[pl.Expr], list[FromExpr], list[GroupExpr]]

class SelectDSLVisitor(NodeVisitor):
    """Class for parsing the `select` DSL. NOTE: outputs with `parse_select_dsl`"""

    # === Top-level ===
    def visit_select_expr(self, node: Node, visited_children: Sequence[Any]) -> SelectDslTreeResult:
        """
        # TODO: How does this handle recursive structure? Does it need to?
        #       ... to start: KISS!
        Parses the select expression and returns a list of operations to apply.

         Each operation contains 3 things:
            1. A list of polars expressions
            2. (if present) A tuple of join operations to apply in-order
            3. (if present) A tuple of other operations to apply in-rder
        """
        # Process `colname_expr`
        colname_expr_list: list[pl.Expr] = []

        # Process `(from_expr)?`
        from_expr_list: list[JoinExpr] = []

        # Process `(table_expr)?`
        table_expr_list: list[GroupExpr] = []

        return (colname_expr_list, from_expr_list, table_expr_list)

    # === Actionable Units ===
    def visit_colname_expr(self, node: Node, visited_children: Sequence[Any]) -> list[pl.Expr]:
        """ """
        ...

    def visit_from_expr(self, node: Node, visited_children: Sequence[Any]):
        ...

    def visit_op_expr(self, node: Node, visited_children: Sequence[Any]):
        ...

    # === Intermediate Representation ===
    def visit_name_arrow(self, node: Node, visited_children: Sequence[Any]):
        ...
    
    def filter_expr(self, node: Node, visited_children: Sequence[Any]):
        ...

    def visit_get_expr(self, node: Node, visited_children: Sequence[Any]):
        return node.text

    ...

    # === Primitives / Lexemes (non-ignored) ===
    def visit_name(self, node: Node, visited_children: Sequence[Any]) -> str:
        return node.text

    # === ... everything else ===
    def generic_visit(
        self, node: Node, visited_children: Sequence[Any]
    ) -> Sequence[Any] | Any | None:
        """Default handler for unspecified rules"""
        # Generic behavior: return either
        #   1) multiple remaining child nodes
        #   2) a single remaining child node
        #   3) `None` if there's no children
        if len(visited_children) > 1:
            return visited_children
        elif len(visited_children) == 1:
            return visited_children[0]
        else:
            return None


def parse_select_dsl(key: str) -> tuple[list[pl.Expr], list[JoinExpr | UnionExpr], list[GroupExpr]]:
    """
    Parses the corresponding key and returns a tuple containing:
      1. colname_expr: A list of polars expressions
      2. from_expr: A list of join expressions to apply
      3. table_expr: A list of grouped operations to apply

    Grammar is defined in `dataframes/dsl.peg`
    """
    # Split into different parts
    FROM_SPLIT = r"\s+from\s+"
    GROUP_SPLIT = "=>"
    contains_from_expr = re.search(FROM_SPLIT, key, re.I)
    contains_group_expr = re.search(GROUP_SPLIT, key)
    if contains_from_expr and contains_group_expr:
        # TODO
        colname_expr_list = ...
        from_expr_list = ...
        group_expr_list = ...
    elif contains_from_expr:
        # TODO
        colname_expr_list = ...
        from_expr_list = ...
        group_expr_list = []
    elif contains_group_expr:
        # TODO
        colname_expr_list = ...
        from_expr_list = []
        group_expr_list = ...
    else:
        # TODO
        colname_expr = ...
        colname_expr_list = []
        from_expr_list, group_expr_list = [], []

    
    parsed_tree = SELECT_DSL_GRAMMAR.parse(key.replace(" ", ""))
    # TODO: Split this out into different visitors
    res = SelectDSLVisitor().visit(parsed_tree)
    return (colname_expr_list, from_expr_list, group_expr_list)


class PythonExprToPolarsExprVisitor(ast.NodeVisitor):
    """Used to generate polars filter expression (borrowing python AST syntax)"""

    def visit_BoolOp(self, node):
        if isinstance(node.op, ast.And):
            expr = self.visit(node.values[0])
            for value in node.values[1:]:
                expr = expr & self.visit(value)
        elif isinstance(node.op, ast.Or):
            expr = self.visit(node.values[0])
            for value in node.values[1:]:
                expr = expr | self.visit(value)
        else:
            raise RuntimeError(f"Got unexpected BoolOp: {node.op}")
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


def generate_polars_filter(filter_string: str) -> pl.Expr:
    """
    Takes an input string and converts it to a polars filter expression
      e.g. `a > 0` -> `pl.col('a') > pl.lit(0)`

    Uses python's AST -- so expecting python syntax
    """
    tree = ast.parse(filter_string, mode="eval")
    visitor = PythonExprToPolarsExprVisitor()
    return visitor.visit(tree.body)


def apply_nested_col_list(
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
                    f"Got unexpected type in `apply_nested_col_list`: {type(col_str)}"
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
