from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

# Load the grammar from the .peg file
GRAMMAR_PATH = Path(__file__).parent / "from.peg"
with open(GRAMMAR_PATH) as f:
    FROM_GRAMMAR = Grammar(f.read())

class FromTransformer(NodeVisitor):
    """Transform the parse tree into a structured representation."""
    
    def visit_from_expr(self, node: Node, visited_children: List) -> Union[str, Dict[str, Any]]:
        """Process the root from expression."""
        [expr, _] = visited_children  # Ignore the end-of-input match
        [result] = expr  # Unpack the actual expression
        if isinstance(result, str):  # Simple table alias
            return result
        return result

    def visit_join_expr(self, node: Node, visited_children: List) -> Dict[str, Any]:
        """Process a join expression."""
        [base_table_or_empty, join_parts] = visited_children
        # Handle empty alias case
        base_table = "A"  # Default to "A"
        if base_table_or_empty:
            [alias] = base_table_or_empty
            if alias:  # If not empty string
                base_table = alias
            
        if not join_parts:  # Just a table alias
            return base_table
            
        # First join creates the simple_join structure
        first_join = join_parts[0]  # Get first join part
        result = {
            "type": "simple_join",
            "left": base_table,
            "op": first_join["op"],
            "right": first_join["table"]
        }
        if "on" in first_join:
            result["on"] = first_join["on"]
            
        # Additional joins create the nested structure
        if len(join_parts) > 1:
            result = {
                "type": "join_expr",
                "base": result,
                "joins": []
            }
            for join_info in join_parts[1:]:
                join = {
                    "op": join_info["op"],
                    "table": join_info["table"]
                }
                if "on" in join_info:
                    join["on"] = join_info["on"]
                result["joins"].append(join)
                
        return result

    def visit_join_part(self, node: Node, visited_children: List) -> Dict[str, Any]:
        """Process a join part."""
        [join_op, table, on_expr] = visited_children
        result = {
            "op": join_op,
            "table": table
        }
        if on_expr:
            [on_info] = on_expr
            result["on"] = on_info
        return result

    def visit_union_expr(self, node: Node, visited_children: List) -> Dict[str, Any]:
        """Process a union expression."""
        first_table, rest = visited_children
        tables = [first_table]
        for union_parts in rest:
            _, table = union_parts
            tables.append(table)
        return {
            "type": "union_expr",
            "tables": tables
        }

    def visit_subquery(self, node: Node, visited_children: List) -> Dict[str, Any]:
        """Process a subquery."""
        [lparen, select_expr, rparen] = visited_children
        return {
            "type": "subquery",
            "query": select_expr
        }

    def visit_select_expr(self, node: Node, visited_children: List) -> str:
        """Process a select expression."""
        return node.text

    def visit_join_op(self, node: Node, visited_children: List) -> str:
        """Process a join operator."""
        [op] = visited_children
        return op

    def visit_on_expr(self, node: Node, visited_children: List) -> Dict[str, List[str]]:
        """Process an on expression."""
        [_, lbrack, name_list, rbrack] = visited_children
        return {
            "type": "on_expr",
            "columns": name_list
        }

    def visit_name_list(self, node: Node, visited_children: List) -> List[str]:
        """Process a list of names."""
        first_name, rest = visited_children
        # Extract the actual name if it's wrapped in a list
        names = [first_name[0] if isinstance(first_name, list) else first_name]
        if rest:
            for comma_name in rest:
                _, name = comma_name
                # Extract the actual name if it's wrapped in a list
                name_value = name[0] if isinstance(name, list) else name
                names.append(name_value)
        return names

    def visit_empty_alias(self, node: Node, visited_children: List) -> str:
        """Process an empty table alias."""
        return ""

    def visit_table_alias(self, node: Node, visited_children: List) -> str:
        """Process a table alias."""
        return node.text.strip()

    def visit_name(self, node: Node, visited_children: List) -> str:
        """Process a name."""
        return node.text.strip()

    def generic_visit(self, node: Node, visited_children: List) -> Any:
        """Default visitor for nodes we don't need to transform."""
        return visited_children or node.text

def parse_from(expr: str) -> Dict[str, Any]:
    """Parse a from expression into a structured representation.
    
    Args:
        expr: The from expression to parse
        
    Returns:
        A dictionary containing the parsed expression structure
    """
    # Remove whitespace as grammar assumes it's been stripped
    expr = expr.replace(" ", "")
    tree = FROM_GRAMMAR.parse(expr)
    transformer = FromTransformer()
    return transformer.visit(tree)
