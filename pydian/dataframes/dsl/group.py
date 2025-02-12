from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

# Load the grammar from the .peg file
GRAMMAR_PATH = Path(__file__).parent / "group.peg"
with open(GRAMMAR_PATH) as f:
    GROUP_GRAMMAR = Grammar(f.read())

class GroupTransformer(NodeVisitor):
    """Transform the parse tree into a structured representation."""
    
    def visit_table_expr(self, node: Node, visited_children: List) -> Dict[str, Any]:
        """Process the root table expression."""
        [groupby_expr, orderby_expr] = visited_children
        result = {"type": "table_expr"}
        
        if groupby_expr:
            [group_info] = groupby_expr
            result["groupby"] = group_info
            
        if orderby_expr:
            [order_info] = orderby_expr
            result["orderby"] = order_info
            
        if not groupby_expr and not orderby_expr:
            raise ValueError("Either groupby or orderby expression is required")
            
        return result

    def visit_groupby_expr(self, node: Node, visited_children: List) -> Dict[str, Any]:
        """Process a groupby expression."""
        [_, lbrack, group_spec, rbrack] = visited_children
        result = {
            "type": "groupby",
            "columns": group_spec["columns"]
        }
        if "aggs" in group_spec:
            result["aggs"] = group_spec["aggs"]
        return result

    def visit_orderby_expr(self, node: Node, visited_children: List) -> Dict[str, List[str]]:
        """Process an orderby expression."""
        [_, lbrack, name_list, rbrack] = visited_children
        return {
            "type": "orderby",
            "columns": name_list
        }

    def visit_group_spec(self, node: Node, visited_children: List) -> Dict[str, Any]:
        """Process a group specification."""
        [name_list, agg_part] = visited_children
        result = {"columns": name_list}
        if agg_part:
            [[_, agg_list]] = agg_part
            result["aggs"] = agg_list
        return result

    def visit_agg_list(self, node: Node, visited_children: List) -> List[str]:
        """Process a list of aggregation functions."""
        [first_agg, rest] = visited_children
        aggs = [first_agg]
        if rest:
            for comma_agg in rest:
                _, agg = comma_agg
                aggs.append(agg)
        return aggs

    def visit_agg_fn(self, node: Node, visited_children: List) -> str:
        """Process an aggregation function."""
        [name, lparen, rparen] = visited_children
        return f"{name}()"

    def visit_name_list(self, node: Node, visited_children: List) -> List[str]:
        """Process a list of names."""
        first_name, rest = visited_children
        names = [first_name]
        if rest:
            for comma_name in rest:
                _, name = comma_name
                names.append(name)
        return names

    def visit_name(self, node: Node, visited_children: List) -> str:
        """Process a name."""
        return node.text.strip()

    def generic_visit(self, node: Node, visited_children: List) -> Any:
        """Default visitor for nodes we don't need to transform."""
        return visited_children or node.text

def parse_group(expr: str) -> Dict[str, Any]:
    """Parse a table expression into a structured representation.
    
    Args:
        expr: The table expression to parse
        
    Returns:
        A dictionary containing the parsed expression structure
        
    Raises:
        ValueError: If neither groupby nor orderby expression is present
    """
    # Remove whitespace as grammar assumes it's been stripped
    expr = expr.replace(" ", "")
    tree = GROUP_GRAMMAR.parse(expr)
    transformer = GroupTransformer()
    return transformer.visit(tree)
