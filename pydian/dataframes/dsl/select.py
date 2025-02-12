from typing import Any, Dict, Optional, Tuple

from .colnames import parse_colnames
from .from_expr import parse_from
from .group import parse_group

def split_query(query: str) -> Tuple[str, str, Optional[str]]:
    """Split a query string into its three components.
    
    Args:
        query: The full query string in format "colname_expr FROM from_expr => group_expr"
        
    Returns:
        Tuple of (colname_expr, from_expr, group_expr)
        group_expr may be None if not provided
    """
    # First split on FROM
    parts = query.split("FROM", 1)
    if len(parts) != 2:
        raise ValueError("Query must contain FROM clause")
    
    colnames_part = parts[0].strip()
    rest = parts[1].strip()
    
    # Then split rest on =>
    parts = rest.split("=>", 1)
    from_part = parts[0].strip()
    group_part = parts[1].strip() if len(parts) > 1 else None
    
    return colnames_part, from_part, group_part

def parse_select(query: str) -> Dict[str, Any]:
    """Parse a full select query into a structured representation.
    
    The query should be in the format:
    "colname_expr FROM from_expr => group_expr"
    
    Args:
        query: The select query to parse
        
    Returns:
        A dictionary containing the parsed query structure with all components
    """
    colnames_part, from_part, group_part = split_query(query)
    
    result = {
        "type": "select",
        "columns": parse_colnames(colnames_part),
        "from": parse_from(from_part)
    }
    
    if group_part:
        result["group"] = parse_group(group_part)
        
    return result
