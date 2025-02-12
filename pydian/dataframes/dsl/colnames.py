from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

# Load the grammar from the .peg file
GRAMMAR_PATH = Path(__file__).parent / "colnames.peg"
with open(GRAMMAR_PATH) as f:
    COLNAMES_GRAMMAR = Grammar(f.read())

class ColNamesTransformer(NodeVisitor):
    """Transform the parse tree into a structured representation."""
    
    def visit_colname_expr(self, node: Node, visited_children: List) -> Dict[str, Any]:
        """Process the root column names expression."""
        [cols_or_star, filter_expr] = visited_children
        result = {"type": "colname_expr"}
        
        # Handle columns
        if isinstance(cols_or_star[0], dict) and cols_or_star[0].get("type") == "star":
            result["columns"] = cols_or_star[0]
        else:
            result["columns"] = cols_or_star[0]
            
        # Handle filter expression
        if filter_expr:
            [filter_info] = filter_expr
            result["filter"] = filter_info
            
        return result

    def visit_colname_list(self, node: Node, visited_children: List) -> List[Dict[str, Any]]:
        """Process a list of column names."""
        first_expr, rest = visited_children
        exprs = [first_expr]
        if rest:
            for comma_expr in rest:
                _, expr = comma_expr
                exprs.append(expr)
        return exprs

    def visit_filter_expr(self, node: Node, visited_children: List) -> Dict[str, str]:
        """Process a filter expression."""
        [colon, lbrack, filter_cols, rbrack] = visited_children
        return {"type": "filter", "expr": filter_cols}

    def visit_name_expr(self, node: Node, visited_children: List) -> Dict[str, Any]:
        """Process a name expression with optional arrow operation."""
        [name_or_nested, arrow_op] = visited_children
        if isinstance(name_or_nested, list):
            [name] = name_or_nested
        else:
            name = name_or_nested
        result = {"type": "name_expr", "name": name}
        if arrow_op:
            [arrow_info] = arrow_op
            result["arrow"] = arrow_info
        return result

    def visit_nested_name(self, node: Node, visited_children: List) -> str:
        """Process a nested name."""
        [base_name, rest] = visited_children
        parts = [base_name]
        for dot_name in rest:
            [_, name_with_index] = dot_name
            parts.append(name_with_index)
        return ".".join(parts)

    def visit_name_with_index(self, node: Node, visited_children: List) -> str:
        """Process a name with optional array index."""
        [name, array_index] = visited_children
        if array_index:
            [index] = array_index
            return f"{name}{index}"
        return name

    def visit_array_index(self, node: Node, visited_children: List) -> str:
        """Process an array index."""
        [lbrack, index, rbrack] = visited_children
        return f"[{index}]"

    def visit_arrow_op(self, node: Node, visited_children: List) -> Dict[str, List[str]]:
        """Process an arrow operation."""
        [arrow, lbrack, name, rest, rbrack] = visited_children
        targets = [name]
        if rest:
            for comma_name in rest:
                _, target = comma_name
                targets.append(target)
        return {"type": "arrow", "targets": targets}

    def visit_name(self, node: Node, visited_children: List) -> str:
        """Process a name."""
        return node.text.strip()

    def visit_star(self, node: Node, visited_children: List) -> Dict[str, str]:
        """Process the star operator."""
        return {"type": "star"}

    def generic_visit(self, node: Node, visited_children: List) -> Any:
        """Default visitor for nodes we don't need to transform."""
        return visited_children or node.text

def parse_colnames(expr: str) -> Dict[str, Any]:
    """Parse a column names expression into a structured representation.
    
    Args:
        expr: The column names expression to parse
        
    Returns:
        A dictionary containing the parsed expression structure
    """
    # Remove whitespace as grammar assumes it's been stripped
    expr = expr.replace(" ", "")
    tree = COLNAMES_GRAMMAR.parse(expr)
    transformer = ColNamesTransformer()
    return transformer.visit(tree)
