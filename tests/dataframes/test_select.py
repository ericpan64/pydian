import pytest

from pydian.dataframes.dsl.from_expr import parse_from
from pydian.dataframes.dsl.group import parse_group
from pydian.dataframes.dsl.select import parse_select

# === From Expression Tests ===

def test_simple_from():
    result = parse_from("A")
    assert result == "A"

def test_simple_join():
    result = parse_from("A<-B on[id]")
    assert result == {
        "type": "simple_join",
        "left": "A",
        "op": "<-",
        "right": "B",
        "on": {
            "type": "on_expr",
            "columns": ["id"]
        }
    }

def test_multiple_joins():
    result = parse_from("A<-B on[id]<>C on[code]")
    assert result == {
        "type": "join_expr",
        "base": {
            "type": "simple_join",
            "left": "A",
            "op": "<-",
            "right": "B",
            "on": {
                "type": "on_expr",
                "columns": ["id"]
            }
        },
        "joins": [{
            "op": "<>",
            "table": "C",
            "on": {
                "type": "on_expr",
                "columns": ["code"]
            }
        }]
    }

def test_union():
    result = parse_from("A++B++C")
    assert result == {
        "type": "union_expr",
        "tables": ["A", "B", "C"]
    }

def test_subquery():
    result = parse_from("(some_select_expr)")
    assert result == {
        "type": "subquery",
        "query": "some_select_expr"
    }

# === Group Expression Tests ===

def test_simple_groupby():
    result = parse_group("groupby[col1]")
    assert result == {
        "type": "table_expr",
        "groupby": {
            "type": "groupby",
            "columns": ["col1"]
        }
    }

def test_multiple_groupby():
    result = parse_group("groupby[col1,col2]")
    assert result == {
        "type": "table_expr",
        "groupby": {
            "type": "groupby",
            "columns": ["col1", "col2"]
        }
    }

def test_orderby():
    result = parse_group("orderby[col1]")
    assert result == {
        "type": "table_expr",
        "orderby": {
            "type": "orderby",
            "columns": ["col1"]
        }
    }

def test_groupby_and_orderby():
    result = parse_group("groupby[col1]orderby[col2]")
    assert result == {
        "type": "table_expr",
        "groupby": {
            "type": "groupby",
            "columns": ["col1"]
        },
        "orderby": {
            "type": "orderby",
            "columns": ["col2"]
        }
    }

# === Full Select Tests ===

def test_simple_select():
    result = parse_select("col1,col2 FROM A")
    assert result == {
        "type": "select",
        "columns": {
            "type": "colname_expr",
            "columns": [
                {"type": "name_expr", "name": "col1"},
                {"type": "name_expr", "name": "col2"}
            ]
        },
        "from": "A"
    }

def test_select_with_join():
    result = parse_select("* FROM A<-B on[id]")
    assert result == {
        "type": "select",
        "columns": {
            "type": "colname_expr",
            "columns": {"type": "star"}
        },
        "from": {
            "type": "simple_join",
            "left": "A",
            "op": "<-",
            "right": "B",
            "on": {
                "type": "on_expr",
                "columns": ["id"]
            }
        }
    }

def test_select_with_group():
    result = parse_select("col1,col2 FROM A => groupby[col1]")
    assert result == {
        "type": "select",
        "columns": {
            "type": "colname_expr",
            "columns": [
                {"type": "name_expr", "name": "col1"},
                {"type": "name_expr", "name": "col2"}
            ]
        },
        "from": "A",
        "group": {
            "type": "table_expr",
            "groupby": {
                "type": "groupby",
                "columns": ["col1"]
            }
        }
    }

def test_complex_select():
    result = parse_select("col1->[new1],col2->[new2] FROM A<-B on[id] => groupby[col1]orderby[new1]")
    assert result == {
        "type": "select",
        "columns": {
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
            ]
        },
        "from": {
            "type": "simple_join",
            "left": "A",
            "op": "<-",
            "right": "B",
            "on": {
                "type": "on_expr",
                "columns": ["id"]
            }
        },
        "group": {
            "type": "table_expr",
            "groupby": {
                "type": "groupby",
                "columns": ["col1"]
            },
            "orderby": {
                "type": "orderby",
                "columns": ["new1"]
            }
        }
    }
