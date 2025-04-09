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
from .dsl.colnames import parse_colnames
from .dsl.from_expr import parse_from
from .dsl.group import parse_group

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
class TableExpr:
    op_type: Literal["GROUPBY", "ORDERBY"]
    on_cols: list[str]
    agg_fns: list[pl.Expr] | None = None


SelectDslTreeResult: TypeAlias = tuple[list[pl.Expr], list[FromExpr], list[TableExpr]]


class ColnameDSLVisitor(NodeVisitor):
    """Visitor for parsing column name expressions in the select DSL."""

    grammar = COLNAMES_DSL_GRAMMAR

    def visit_colname_expr(
        self, node: Node, visited_children: Sequence[Any]
    ) -> list[pl.Expr] | KEEP:
        """Handle expressions like 'a, b, c' or '*'."""
        cols, filter_expr = visited_children
        if filter_expr:
            if isinstance(cols, KEEP):
                return KEEP(filter_expr[0])
            return [col.filter(filter_expr[0]) for col in flatten_sequence(cols)]
        return cols

    def visit_colname_list(self, node: Node, visited_children: Sequence[Any]) -> list[pl.Expr]:
        """Handle comma-separated column lists."""
        first, rest = visited_children
        return [first] + [col for _, col in rest]

    def visit_name_expr(
        self, node: Node, visited_children: Sequence[Any]
    ) -> pl.Expr | list[pl.Expr]:
        """Handle column names with optional arrow operations."""
        name, arrow_op = visited_children
        if arrow_op:
            nested_cols = arrow_op[0]
            return [pl.col(name).struct.field(col) for col in nested_cols]
        return pl.col(name)

    def visit_star(self, node: Node, visited_children: Sequence[Any]) -> KEEP:
        """Handle '*' expressions."""
        return KEEP("*")

    def visit_filter_expr(self, node: Node, visited_children: Sequence[Any]) -> ast.Expression:
        """Handle filter expressions in square brackets."""
        _, _, filter_cols, _ = visited_children
        return ast.parse(filter_cols.text.strip(), mode="eval").body

    def generic_visit(
        self, node: Node, visited_children: Sequence[Any]
    ) -> Sequence[Any] | Any | None:
        """Default handler for unspecified rules."""
        return visited_children or node.text


class FromDSLVisitor(NodeVisitor):
    """Visitor for parsing table operations in the from clause."""

    grammar = FROM_DSL_GRAMMAR

    def visit_from_expr(
        self, node: Node, visited_children: Sequence[Any]
    ) -> list[JoinExpr | UnionExpr]:
        """Handle top-level from expressions."""
        return visited_children[0]

    def visit_join_expr(self, node: Node, visited_children: Sequence[Any]) -> list[JoinExpr]:
        """Handle join expressions."""
        base, joins = visited_children[0], visited_children[1]
        result = [base]
        for join in joins:
            if join:
                result.append(join)
        return result

    def visit_simple_join(self, node: Node, visited_children: Sequence[Any]) -> JoinExpr:
        """Handle basic join operations."""
        lhs, join_op, rhs, on_expr = visited_children
        join_type: Literal["LEFT", "INNER"] = "LEFT" if join_op == "<-" else "INNER"
        return JoinExpr(lhs, rhs, join_type, on_expr)

    def visit_union_expr(self, node: Node, visited_children: Sequence[Any]) -> list[UnionExpr]:
        """Handle union operations."""
        lhs, unions = visited_children
        return [UnionExpr(lhs, rhs) for _, rhs in unions]

    def generic_visit(
        self, node: Node, visited_children: Sequence[Any]
    ) -> Sequence[Any] | Any | None:
        """Default handler for unspecified rules."""
        return visited_children or node.text


class GroupDSLVisitor(NodeVisitor):
    """Visitor for parsing group and order operations."""

    grammar = GROUP_DSL_GRAMMAR

    def visit_table_expr(self, node: Node, visited_children: Sequence[Any]) -> list[GroupExpr]:
        """Handle top-level group/order expressions."""
        groupby, orderby = visited_children
        result = []
        if groupby and groupby[0]:
            result.append(groupby[0])
        if orderby and orderby[0]:
            result.append(orderby[0])
        return result

    def visit_groupby_expr(self, node: Node, visited_children: Sequence[Any]) -> GroupExpr:
        """Handle groupby expressions."""
        _, _, cols, _ = visited_children
        final_cols = [col.value if isinstance(col, KEEP) else col for col in cols]
        return GroupExpr("GROUPBY", final_cols, [])

    def visit_orderby_expr(self, node: Node, visited_children: Sequence[Any]) -> GroupExpr:
        """Handle orderby expressions."""
        _, _, cols, _ = visited_children
        final_cols = [col.value if isinstance(col, KEEP) else col for col in cols]
        return GroupExpr("ORDERBY", final_cols, [])

    def generic_visit(
        self, node: Node, visited_children: Sequence[Any]
    ) -> Sequence[Any] | Any | None:
        """Default handler for unspecified rules."""
        return visited_children or node.text


def parse_select_dsl(key: str) -> SelectDslTreeResult:
    """Parse a select DSL expression into its components."""
    # Clean input
    key = re.sub(r"\s+", " ", key.strip())

    # Split into components
    parts = re.split(r"\s+from\s+|\s*=>\s*", key, flags=re.I)

    if len(parts) == 3:  # All components present
        colname_part, from_part, group_part = parts
    elif len(parts) == 2:
        if "=>" in key:
            colname_part, group_part = parts
            from_part = None
        else:
<<<<<<< HEAD
            return None


def parse_select_dsl(key: str) -> tuple[list[pl.Expr], list[JoinExpr | UnionExpr], list[TableExpr]]:
    """
    Parses the corresponding key and returns a tuple containing:
      1. colname_expr: A list of polars expressions
      2. from_expr: A list of join expressions to apply
      3. table_expr: A list of grouped operations to apply

    Grammar is defined in the DSL .peg files
    """
    # Split into different parts
    FROM_SPLIT = r"\s+from\s+"
    GROUP_SPLIT = "=>"
    
    # First split on FROM
    parts = re.split(FROM_SPLIT, key, flags=re.I, maxsplit=1)
    colnames_part = parts[0].strip()
    from_part = parts[1].strip() if len(parts) > 1 else None
    
    # Parse colnames
    colname_result = parse_colnames(colnames_part)
    colname_expr_list = []
    
    # Handle star operator first
    if isinstance(colname_result["columns"], dict) and colname_result["columns"]["type"] == "star":
        expr = pl.col("*")
        if "filter" in colname_result:
            expr = expr.filter(generate_polars_filter(colname_result["filter"]["expr"]))
        colname_expr_list = [expr]
    else:
        # Handle column list
        cols = colname_result["columns"]
        if not isinstance(cols, list):
            cols = [cols]
        
        # Process each column
        for col in cols:
            name = col["name"]
            expr = pl.col(name)
            
            # Handle nested access
            if "." in name:
                parts = name.split(".")
                expr = pl.col(parts[0])
                try:
                    for part in parts[1:]:
                        expr = expr.struct.field(part)
                    expr = expr.alias(name)
                except pl.StructFieldNotFoundError:
                    # If struct field not found, try treating it as a regular column
                    expr = pl.col(name)
            
            # Handle arrow operations (column renaming)
            if "arrow" in col:
                for target in col["arrow"]["targets"]:
                    expr = expr.alias(target)
            
            # Apply filter if present
            if "filter" in colname_result:
                expr = expr.filter(generate_polars_filter(colname_result["filter"]["expr"]))
            
            colname_expr_list.append(expr)
    
    # Handle from and group parts if present
    from_expr_list = []
    group_expr_list = []
    
    if from_part:
        # Split on => if present
        group_parts = from_part.split(GROUP_SPLIT, 1)
        from_part = group_parts[0].strip()
        
        # Parse from expression
        from_result = parse_from(from_part)
        if isinstance(from_result, dict):
            if from_result["type"] == "simple_join":
                # Validate join columns exist in both tables
                if "on" not in from_result:
                    raise ValueError("Join operation requires 'on' clause")
                join_cols = from_result["on"]["columns"]
                from_expr_list.append(JoinExpr(
                    lhs=from_result["left"],
                    rhs=from_result["right"],
                    join_type="LEFT" if from_result["op"] == "<-" else "INNER",
                    on_cols=join_cols
                ))
            elif from_result["type"] == "union_expr":
                tables = from_result["tables"]
                # Create union expressions between consecutive tables
                from_expr_list.extend(
                    UnionExpr(lhs=tables[i], rhs=tables[i+1])
                    for i in range(len(tables)-1)
                )
        
        # Parse group expression if present
        if len(group_parts) > 1:
            group_part = group_parts[1].strip()
            group_result = parse_group(group_part)
            if "groupby" in group_result:
                group_info = group_result["groupby"]
                agg_fns = None
                if "aggs" in group_info:
                    agg_fns = []
                    for agg in group_info["aggs"]:
                        if agg == "all()":
                            agg_fns.append(pl.all().name.suffix("_all"))
                        elif agg == "n_unique()":
                            agg_fns.append(pl.n_unique("*").name.suffix("_n_unique"))
                        elif agg == "len()":
                            agg_fns.append(pl.len().name.suffix("_len"))
                        elif agg == "sum()":
                            agg_fns.append(pl.sum().name.suffix("_sum"))
                        elif agg == "mean()":
                            agg_fns.append(pl.mean().name.suffix("_mean"))
                        elif agg == "max()":
                            agg_fns.append(pl.max().name.suffix("_max"))
                        elif agg == "min()":
                            agg_fns.append(pl.min().name.suffix("_min"))
                        elif agg == "median()":
                            agg_fns.append(pl.median().name.suffix("_median"))
                group_expr_list.append(TableExpr(
                    op_type="GROUPBY",
                    on_cols=group_info["columns"],
                    agg_fns=agg_fns
                ))
            if "orderby" in group_result:
                group_expr_list.append(TableExpr(
                    op_type="ORDERBY",
                    on_cols=group_result["orderby"]["columns"]
                ))
    
=======
            colname_part, from_part = parts
            group_part = None
    else:
        colname_part = parts[0]
        from_part = group_part = None

    # Parse each component
    colname_visitor = ColnameDSLVisitor()
    colname_expr_list = colname_visitor.visit(COLNAMES_DSL_GRAMMAR.parse(colname_part))

    from_expr_list = []
    if from_part:
        from_visitor = FromDSLVisitor()
        from_expr_list = from_visitor.visit(FROM_DSL_GRAMMAR.parse(from_part))

    group_expr_list = []
    if group_part:
        group_visitor = GroupDSLVisitor()
        group_expr_list = group_visitor.visit(GROUP_DSL_GRAMMAR.parse(group_part))

>>>>>>> 97cfa4773b82485c69ab21fbdbec758638c564b3
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
