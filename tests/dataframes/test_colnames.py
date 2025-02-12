import pytest

from pydian.dataframes.dsl.colnames import parse_colnames

def test_simple_colname():
    result = parse_colnames("col1")
    assert result == {
        "type": "colname_expr",
        "columns": [{
            "type": "name_expr",
            "name": "col1"
        }]
    }

def test_multiple_colnames():
    result = parse_colnames("col1,col2")
    assert result == {
        "type": "colname_expr",
        "columns": [
            {"type": "name_expr", "name": "col1"},
            {"type": "name_expr", "name": "col2"}
        ]
    }

def test_star_operator():
    result = parse_colnames("*")
    assert result == {
        "type": "colname_expr",
        "columns": {"type": "star"}
    }

def test_arrow_operation():
    result = parse_colnames("col1->[new_name]")
    assert result == {
        "type": "colname_expr",
        "columns": [{
            "type": "name_expr",
            "name": "col1",
            "arrow": {
                "type": "arrow",
                "targets": ["new_name"]
            }
        }]
    }

def test_filter_expression():
    result = parse_colnames("col1:[x > 0]")
    assert result == {
        "type": "colname_expr",
        "columns": [{
            "type": "name_expr",
            "name": "col1"
        }],
        "filter": {
            "type": "filter",
            "expr": "x > 0"
        }
    }

def test_complex_expression():
    result = parse_colnames("col1->[new1],col2->[new2]:[x > 0]")
    assert result == {
        "type": "colname_expr",
        "columns": [
            {
                "type": "name_expr",
                "name": "col1",
                "arrow": {
                    "type": "arrow",
                    "targets": ["new1"]
                }
            },
            {
                "type": "name_expr",
                "name": "col2",
                "arrow": {
                    "type": "arrow",
                    "targets": ["new2"]
                }
            }
        ],
        "filter": {
            "type": "filter",
            "expr": "x > 0"
        }
    }
