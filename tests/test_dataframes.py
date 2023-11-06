from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

import pydian.partials as p
from pydian.dataframes import alter, inner_join, insert, left_join, select


def test_select(simple_dataframe: pd.DataFrame) -> None:
    source = simple_dataframe

    assert source[["a"]].equals(select(source, "a"))
    assert source[["a", "b"]].equals(select(source, "a, b"))
    assert source[["a", "a"]].equals(select(source, "a, a"))
    assert source.equals(select(source, "*"))

    assert select(source, "non_existant_col", apply=p.equals("thing")) is None
    assert select(source, "non_existant_col", default="thing", apply=p.equals("thing")) == True
    assert select(source, "non_existant_col", consume=True) is None
    assert (
        select(
            source,
            "non_existant_col",
            default="",
            apply=p.equals("thing"),
            only_if=p.equals("thing"),
        )
        is None
    )

    # A single non-existant column will cause the entire operation to fail and return None
    #   Most of the times, we expect columns to be persistent (i.e. no "optional" cases)
    assert select(source, "a, non_existant_col") is None

    # # Query syntax (WHERE in SQL, filter in Pandas)
    q1 = select(source, "a ~ [a == 0]")
    q2 = select(source, "a, b, c ~ [a % 2 == 0]")
    q3_none = select(source, "non_existant_col ~ [a % 2 == 0]")
    assert pd.DataFrame(source[source["a"] == 0]["a"]).equals(q1)
    assert source[source["a"] % 2 == 0][["a", "b", "c"]].equals(q2)
    assert q3_none is None

    # Replace
    assert source.where(lambda r: r["a"] % 2 == 0, other="Test").equals(
        select(
            source,
            "*",
            apply=p.replace_where(lambda r: r["a"] % 2 == 0, "Test"),
        )
    )

    # ORDER BY
    assert source.sort_values("a", ascending=False).equals(
        select(source, "*", apply=p.order_by("a", False))
    )

    # GROUP BY
    assert source.groupby("a").groups == select(source, "*", apply=p.group_by("a"))

    # "First n"
    assert source.head(5).equals(select(source, "*", apply=p.keep(5)))

    # Distinct
    assert source.drop_duplicates().equals(select(source, "*", apply=p.distinct()))


def test_select_apply_map(simple_dataframe: pd.DataFrame) -> None:
    source = simple_dataframe
    apply_map = {"a": [p.multiply(2), p.add(1)], "b": [str.upper], "d": p.equivalent(None)}

    comp_df = simple_dataframe.copy()
    comp_df["a"] *= 2
    comp_df["a"] += 1
    comp_df["b"] = comp_df["b"].apply(str.upper)
    comp_df["d"] = comp_df["d"].apply(lambda v: v is None)

    assert comp_df.equals(select(source, "*", apply=apply_map))


def test_select_consume(simple_dataframe: pd.DataFrame) -> None:
    source = simple_dataframe
    source_two = deepcopy(simple_dataframe)
    source_ref = deepcopy(simple_dataframe)

    init_mem_usage_by_column = source.memory_usage(deep=True)
    assert source[["a"]].equals(select(source, "a", consume=True))
    assert source.empty == False
    assert "a" not in source.columns
    assert sum(source.memory_usage(deep=True)) < sum(init_mem_usage_by_column)

    # Selecting from a missing column will not consume others specified (operation failed)
    assert select(source, "a, b", consume=True) is None
    assert "b" in source.columns
    assert source_ref["b"].equals(source["b"])

    # Selecting multiple columns that are all valid
    assert source_two.equals(source_ref)
    assert source_two[["b", "c"]].equals(select(source_two, "b, c", consume=True))
    assert "b" not in source_two.columns
    assert "c" not in source_two.columns


def test_nested_select(nested_dataframe: pd.DataFrame) -> None:
    single_nesting_res = select(nested_dataframe, "simple_nesting.patient.id")
    single_nesting_expected = pd.DataFrame(
        nested_dataframe["simple_nesting"].apply(
            lambda r: r["patient"]["id"] if isinstance(r, dict) else None
        )
    )
    single_nesting_expected.columns = ["simple_nesting.patient.id"]
    assert single_nesting_expected.equals(single_nesting_res)


def test_left_join(simple_dataframe: pd.DataFrame) -> None:
    source = simple_dataframe

    df_right = pd.DataFrame(
        {
            "a": [0, 2, 6, 7],
            "e": ["foo", "bar", "baz", "qux"],
        }
    )

    # `None` cases
    assert left_join(source, df_right, on="d") is None, "Expected None since `d` is not in right"
    assert left_join(source, df_right, on="e") is None, "Expected None since `e` is not in left"
    assert left_join(source, df_right, on="f") is None, "Expected None since `f` is not in either"
    assert (
        left_join(source, df_right, on=["a", "f"]) is None
    ), "Expected None since `f` is not in either"
    assert (
        left_join(source, df_right, on=["e", "f"]) is None
    ), "Expected None since `f` is not in either"
    assert (
        left_join(source, df_right, on=["a", "e"]) is None
    ), "Expected None since `e` is not in left"

    # Basic join
    expected = deepcopy(source)
    expected = expected.merge(df_right, on="a", how="left")
    result = left_join(source, df_right, on="a")

    assert expected.equals(result)

    # Join resulting in empty DataFrame
    df_empty_right = pd.DataFrame(columns=["a", "e"])
    result = left_join(source, df_empty_right, on="a")
    assert result is None, "Expected None -- resulting DataFrame is empty"

    # # Test `consume=True`
    # result = left_join(source, df_right, on="a", consume=True)
    # assert expected.equals(result)
    # assert df_right.equals(pd.DataFrame({
    #     "a": [6, 7],
    #     "e": ["baz", "qux"],
    # }))


def test_inner_join(simple_dataframe: pd.DataFrame) -> None:
    # Split the simple_dataframe into two DataFrames for joining
    df1 = simple_dataframe[["a", "b"]]
    df2 = simple_dataframe[["b", "c"]]

    # Expected result of the inner join
    expected_result = pd.DataFrame(
        {
            "a": [0, 1, 2, 3, 4, 5],
            "b": ["q", "w", "e", "r", "t", "y"],
            "c": [True, False, True, False, False, True],
        }
    )

    # Perform the inner join
    result = inner_join(df1, df2, on="b")

    # Check that the result matches the expected result
    pd.testing.assert_frame_equal(result, expected_result)

    # Test with non-existent column
    result = inner_join(df1, df2, on="non_existent_column")
    assert result is None, f"Expected None, but got {result}"

    # Test with empty result
    df1_empty = df1.head(0)
    result = inner_join(df1_empty, df2, on="b")
    assert result is None, f"Expected None, but got {result}"


def test_insert(simple_dataframe: pd.DataFrame) -> None:
    rows_to_insert = [{"a": 6, "b": "u", "c": False, "d": pd.NA}]
    expected_data = {
        "a": [0, 1, 2, 3, 4, 5, 6],
        "b": ["q", "w", "e", "r", "t", "y", "u"],
        "c": [True, False, True, False, False, True, False],
        "d": [pd.NA, None, None, pd.NA, pd.NA, None, pd.NA],
    }

    # Test basic insert functionality
    result = insert(simple_dataframe, rows_to_insert)
    pd.DataFrame(expected_data).equals(result)

    # Test consume functionality
    rows_df = pd.DataFrame(rows_to_insert)
    result = insert(simple_dataframe, rows_df, consume=True)
    pd.DataFrame(expected_data).equals(result)
    assert rows_df.empty, f"Expected rows_df to be empty, but got {rows_df}"

    # Test default value functionality
    rows_to_insert_default = [{"a": 7, "b": "i"}]
    expected_data_default = {
        "a": [0, 1, 2, 3, 4, 5, 7],
        "b": ["q", "w", "e", "r", "t", "y", "i"],
        "c": [True, False, True, False, False, True, pd.NA],
        "d": [pd.NA, None, None, pd.NA, pd.NA, None, pd.NA],
    }
    result = insert(simple_dataframe, rows_to_insert_default)
    pd.DataFrame(expected_data_default).equals(result)

    # Test incompatible columns
    incompatible_rows = [{"e": 8}]
    result = insert(simple_dataframe, incompatible_rows)
    assert result is None, f"Expected None, but got {result}"


def test_alter(simple_dataframe: pd.DataFrame) -> None:
    # Test the drop_cols feature of the alter function
    drop_result: pd.DataFrame = alter(simple_dataframe, drop_cols="a,c")
    assert "a" not in drop_result.columns
    assert "c" not in drop_result.columns

    # Test the overwrite_cols feature of the alter function
    overwrite_result: pd.DataFrame = alter(
        simple_dataframe, overwrite_cols={"b": ["z", "x", "c", "v", "b", "n"]}
    )
    assert all(overwrite_result["b"] == ["z", "x", "c", "v", "b", "n"])

    # Test the add_cols feature of the alter function
    add_result: pd.DataFrame = alter(simple_dataframe, add_cols={"e": [6, 7, 8, 9, 10, 11]})
    assert "e" in add_result.columns
    assert all(add_result["e"] == [6, 7, 8, 9, 10, 11])

    # Test the alter function with invalid input
    with pytest.raises(ValueError):
        alter(simple_dataframe, add_cols="not a dictionary")  # type: ignore
