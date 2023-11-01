import pandas as pd

import pydian.partials as p
from pydian.dataframes import select


def test_select(simple_dataframe: pd.DataFrame) -> None:
    source = simple_dataframe

    assert select(source, "a").equals(source[["a"]])  #  type: ignore
    assert select(source, "a, b").equals(source[["a", "b"]])  #  type: ignore
    assert select(source, "*").equals(source)  #  type: ignore

    assert select(source, "non_existant_col", apply=p.equals("thing")) is None
    assert select(source, "non_existant_col", default="thing", apply=p.equals("thing")) == True
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

    # WHERE
    assert select(  #  type: ignore
        source,
        "*",
        apply=p.where(lambda r: r["a"] % 2 == 0),
    ).equals(source.where(lambda r: r["a"] % 2 == 0))

    # ORDER BY
    assert select(source, "*", apply=p.order_by("a", False)).equals(  #  type: ignore
        source.sort_values("a", ascending=False)
    )  #  type: ignore

    # GROUP BY
    assert select(source, "*", apply=p.group_by("a")) == source.groupby("a").groups

    # "First n"
    assert select(source, "*", apply=p.keep(5)).equals(source.head(5))  #  type: ignore

    # Distinct
    assert select(source, "*", apply=p.distinct()).equals(source.drop_duplicates())  #  type: ignore


def test_select_apply_map(simple_dataframe) -> None:
    source = simple_dataframe
    apply_map = {"a": [p.multiply(2), p.add(1)], "b": [str.upper], "d": p.equivalent(None)}

    comp_df = simple_dataframe.copy()
    comp_df["a"] *= 2
    comp_df["a"] += 1
    comp_df["b"] = comp_df["b"].apply(str.upper)
    comp_df["d"] = comp_df["d"].apply(lambda v: v is None)

    assert select(source, "*", apply=apply_map).equals(comp_df)  #  type: ignore


# TODO
def test_select_consume(simple_dataframe: pd.DataFrame) -> None:
    ...


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
