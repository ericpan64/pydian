from copy import deepcopy

import polars as pl
from polars.testing import (
    assert_frame_equal,  # Do `type: ignore` to gnore the `Err` case
)
from result import Err

from pydian.dataframes.core import select


def test_select(simple_dataframe: pl.DataFrame) -> None:
    source = simple_dataframe

    assert_frame_equal(select(source, "a"), source[["a"]])  # type: ignore
    assert_frame_equal(source[["a", "b"]], select(source, "a, b"))  # type: ignore
    assert_frame_equal(select(source, "*"), source)  # type: ignore

    assert isinstance(select(source, "non_existant_col"), Err)

    # A single non-existant column will cause the entire operation to fail and return None
    #   Most of the times, we expect columns to be persistent (i.e. no "optional" cases)
    assert isinstance(select(source, "a, non_existant_col"), Err)

    # # Query syntax (WHERE in SQL, filter in Polars)
    q1 = select(source, "b : [a == 0]")
    q2 = select(source, "a, b, c : [a % 2 == 0]")
    q3_err = select(source, "non_existant_col : [a % 2 == 0]")

    assert_frame_equal(q1, source.filter(pl.col("a") == 0).select("b"))  # type: ignore
    assert_frame_equal(q2, source.filter(pl.col("a") % 2 == 0).select(["a", "b", "c"]))  # type: ignore
    assert isinstance(q3_err, Err)


def test_nested_select(nested_dataframe: pl.DataFrame) -> None:
    # TODO: Refactor this test using the expected behavior
    source: pl.DataFrame = nested_dataframe

    single_nesting_expected = source.select(
        pl.col("simple_nesting")
        .struct.field("patient")
        .struct.field("id")
        .alias("simple_nesting.patient.id")
    )
    select_single_nesting = select(source, "simple_nesting.patient.id")
    assert_frame_equal(select_single_nesting, single_nesting_expected)  # type: ignore

    multi_nesting_expected = source.select(
        [
            pl.col("simple_nesting")
            .struct.field("patient")
            .struct.field("id")
            .alias("simple_nesting.patient.id"),
            pl.col("deep_nesting")
            .struct.field("patient")
            .struct.field("dicts")
            .list.first()
            .struct.field("inner")
            .struct.field("msg")
            .alias("deep_nesting.patient.dicts[0].inner.msg"),
        ]
    )
    select_multi_nesting = select(
        source, "simple_nesting.patient.id, deep_nesting.patient.dicts[0].inner.msg"
    )
    assert_frame_equal(
        select_multi_nesting,  # type: ignore
        multi_nesting_expected,
    )

    # Extend, and consume source col (->)
    extend_expected = source.select(
        pl.col("simple_nesting")
        .struct.field("patient")
        .struct.field("id")
        .alias("simple_nesting.patient.id"),
        pl.col("simple_nesting")
        .struct.field("patient")
        .struct.field("active")
        .alias("simple_nesting.patient.active"),
    )
    select_extend = select(source, "simple_nesting -> [patient.id, patient.active]")
    assert_frame_equal(select_extend, extend_expected)  # type: ignore


def test_left_join(simple_dataframe: pl.DataFrame) -> None:
    source = simple_dataframe

    df_right = pl.DataFrame(
        {
            "a": [0, 2, 6, 7],
            "e": ["foo", "bar", "baz", "qux"],
        }
    )

    # `None` cases (using shorthand syntax)
    assert isinstance(
        select(source, "* from A <- B ON [d]", others=df_right), Err
    ), "Expected Err since `d` is not in right"
    assert isinstance(
        select(source, "* from A <- B ON [e]", others=df_right), Err
    ), "Expected Err since `e` is not in left"
    assert isinstance(
        select(source, "* from A <- B ON [f]", others=df_right), Err
    ), "Expected Err since `f` is not in either"
    assert isinstance(
        select(source, "* from A <- B ON [a, f]", others=df_right), Err
    ), "Expected Err since `f` is not in either"
    assert isinstance(
        select(source, "* from A <- B on [e, f]", others=df_right), Err
    ), "Expected Err since `f` is not in either"
    assert isinstance(
        select(source, "* from A <- B on [a, e]", others=df_right), Err
    ), "Expected Err since `e` is not in left"

    # Basic join
    expected = deepcopy(source)
    expected = expected.join(df_right, how="left", on="a", coalesce=True)
    result = select(source, "* from A <- B ON [a]", others=df_right)

    assert_frame_equal(expected, result)  # type: ignore

    # Join resulting in empty DataFrame -- return an `Err` so people know that's the case
    df_empty_right = pl.DataFrame(
        {"a": pl.Series([], dtype=pl.Int64), "e": pl.Series([], dtype=pl.Int64)},
    )
    result = select(source, "* from A <- B ON [a]", others=df_empty_right)
    assert isinstance(result, Err), f"Expected Err -- resulting DataFrame is empty, got: {result}"


def test_inner_join(simple_dataframe: pl.DataFrame) -> None:
    # Split the simple_dataframe into two DataFrames for joining
    df1 = simple_dataframe[["a", "b"]]
    df2 = simple_dataframe[["b", "c"]]

    # Expected result of the inner join
    expected_result = pl.DataFrame(
        {
            "a": [0, 1, 2, 3, 4, 5],
            "b": ["q", "w", "e", "r", "t", "y"],
            "c": [True, False, True, False, False, True],
        }
    )

    # Perform the inner join
    result = select(df1, "* from A <> B on [b]", others=df2)

    # Check that the result matches the expected result
    assert_frame_equal(result, expected_result)  # type: ignore

    # Test with non-existent column
    result = select(df1, "* from A <> B on [non_existent_col]", others=df2)
    assert isinstance(result, Err), f"Expected Err, but got {result}"

    # Test with empty result
    df1_empty = df1.head(0)
    result = select(df1_empty, "* from  <> B on [b]", others=df2)
    assert isinstance(result, Err), f"Expected Err, but got {result}"


def test_union(simple_dataframe: pl.DataFrame) -> None:
    rows_to_union = pl.DataFrame({"a": [6], "b": ["u"], "c": [False], "d": [None]})
    expected_data = {
        "a": [0, 1, 2, 3, 4, 5, 6],
        "b": ["q", "w", "e", "r", "t", "y", "u"],
        "c": [True, False, True, False, False, True, False],
        "d": [None, None, None, None, None, None, None],
    }

    # Test basic union functionality
    result = select(simple_dataframe, "* from A ++ B", others=rows_to_union)
    pl.DataFrame(expected_data).equals(result)  # type: ignore

    # Test default value functionality (i.e. if column isn't there, populate a `None`)
    rows_to_union_default = pl.DataFrame({"a": [7], "b": "i"})
    expected_data_default = {
        "a": [0, 1, 2, 3, 4, 5, 7],
        "b": ["q", "w", "e", "r", "t", "y", "i"],
        "c": [True, False, True, False, False, True, None],
        "d": [None, None, None, None, None, None, None],
    }
    result = select(simple_dataframe, "* from A ++ B", others=rows_to_union_default)
    pl.DataFrame(expected_data_default).equals(result)  # type: ignore

    # Test incompatible columns
    incompatible_rows = pl.DataFrame(
        {"e": [8]}
    )  # This column isn't in the original, so `Err` on the union
    result = select(simple_dataframe, "* from A ++ B", others=incompatible_rows)
    assert isinstance(result, Err), f"Expected None, but got {result}"

    # Test selecting some columns
    result = select(simple_dataframe, "a, b from A ++ B", others=rows_to_union)
    pl.DataFrame(expected_data).select([pl.col("a"), pl.col("b")]).equals(result)  # type: ignore


# def test_group_by(simple_dataframe: pl.DataFrame) -> None:
#     # Group column -- default aggregation (`.all()`)
#     group_by_a = group_by(simple_dataframe, "a")  # (`a` has all unique int values)
#     group_by_b = group_by(simple_dataframe, "b")  # (`b` has all unique str values)
#     group_by_c = group_by(simple_dataframe, "c")  # (`c` has half `True`, half `False`)
#     group_by_d = group_by(simple_dataframe, "d")  # (`d` has all `None`)

#     assert_frame_equal(group_by_a, simple_dataframe.group_by("a", maintain_order=True).all())  # type: ignore
#     assert_frame_equal(group_by_b, simple_dataframe.group_by("b", maintain_order=True).all())  # type: ignore
#     assert_frame_equal(group_by_c, simple_dataframe.group_by("c", maintain_order=True).all())  # type: ignore
#     assert_frame_equal(group_by_d, simple_dataframe.group_by("d", maintain_order=True).all())  # type: ignore

#     # Group by multiple columns -- default aggregation (`.all()`)
#     group_by_ab = group_by(simple_dataframe, "a, b")
#     assert_frame_equal(
#         group_by_ab, simple_dataframe.group_by(["a", "b"], maintain_order=True).all()  # type: ignore
#     )

#     group_by_ac = group_by(simple_dataframe, "a, c")
#     assert_frame_equal(
#         group_by_ac, simple_dataframe.group_by(["a", "c"], maintain_order=True).all()  # type: ignore
#     )

#     # Group by with `len()` aggregation
#     group_by_a_len = group_by(simple_dataframe, "a -> ['*'.len()]")
#     assert_frame_equal(
#         group_by_a_len,  # type: ignore
#         simple_dataframe.group_by("a", maintain_order=True).agg(
#             [pl.col("*").len().name.suffix("_len")]
#         ),
#     )

#     # Group by with `sum()` aggregation
#     group_by_a_sum = group_by(simple_dataframe, "b -> ['a'.sum()]")
#     assert_frame_equal(
#         group_by_a_sum,  # type: ignore
#         simple_dataframe.group_by("b", maintain_order=True).agg([pl.col("a").sum().alias("a_sum")]),
#     )

#     # Group by with `mean()` aggregation
#     group_by_a_mean = group_by(simple_dataframe, "b -> ['a'.mean()]")
#     assert_frame_equal(
#         group_by_a_mean,  # type: ignore
#         simple_dataframe.group_by("b", maintain_order=True).agg(
#             [pl.col("a").mean().alias("a_mean")]
#         ),
#     )

#     # Group by with multiple aggregations
#     group_by_a_aggs = group_by(
#         simple_dataframe, "c -> ['a'.sum(), 'a'.mean(), 'a'.min(), 'a'.max()]"
#     )
#     assert_frame_equal(
#         group_by_a_aggs,  # type: ignore
#         simple_dataframe.group_by("c", maintain_order=True).agg(
#             [
#                 pl.col("a").sum().alias("a_sum"),
#                 pl.col("a").mean().alias("a_mean"),
#                 pl.col("a").min().alias("a_min"),
#                 pl.col("a").max().alias("a_max"),
#             ]
#         ),
#     )

#     # Test error handling
#     assert isinstance(group_by(simple_dataframe, "a -> b"), Err)
#     assert isinstance(group_by(simple_dataframe, "a -> ['b'.invalid_agg()]"), Err)
