import pandas as pd

import pydian.partials as p
from pydian.dataframes import select


def test_select(simple_dataframe: pd.DataFrame) -> None:
    source = simple_dataframe

    assert source[["a"]].equals(select(source, "a"))
    assert source[["a", "b"]].equals(select(source, "a, b"))
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

    # WHERE
    assert source.where(lambda r: r["a"] % 2 == 0).equals(
        select(
            source,
            "*",
            apply=p.where(lambda r: r["a"] % 2 == 0),
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

    init_mem_usage_by_column = source.memory_usage(deep=True)
    assert source[["a"]].equals(select(source, "a", consume=True))
    assert "a" not in source.columns
    assert sum(source.memory_usage(deep=True)) < sum(init_mem_usage_by_column)

    # Selecting from a missing column will not consume others specified (operation failed)
    assert select(source, "a, b", consume=True) is None
    assert "b" in source.columns

    # Selecting multiple columns that are all valid
    assert source[["b", "c"]].equals(select(source, "b, c", consume=True))
    assert "b" not in source.columns
    assert "c" not in source.columns


# TODO
def test_select_polars(simple_dataframe: pd.DataFrame) -> None:
    ...


# TODO
def test_join(simple_dataframe: pd.DataFrame) -> None:
    # Pandas and Pandas
    # Pandas and Polars
    # Polars and Pandas
    # Polars and Polars
    ...
