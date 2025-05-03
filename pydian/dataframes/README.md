# pydian.dataframes – DataFrame support with Polars

This module provides an ergonomic API for manipulating `polars.DataFrame` using a concise SQL-like DSL.

## API

- `select(source: pl.DataFrame, key: str, others: pl.DataFrame | list[pl.DataFrame] | None = None, rename: dict[str, str] | Callable[[str], str] | None = None) -> pl.DataFrame | Err`
  - Select, filter, join, union, and group data via string syntax:
    - `*` – all columns
    - `a, b, c` – pick columns
    - `: [cond]` – filter rows (e.g. `: [col > 3]`)
    - `-> [col1, col2]` – unnest struct/list columns
    - `from A <- B on [col]` – left join
    - `from A <> B on [col]` – inner join
    - `++` – union/append
    - `=> groupby[col | sum(), mean()]` – group and aggregate
  - Optional `rename` applied at the end.

## Example

```python
import polars as pl
from pydian.dataframes import select

df1 = pl.DataFrame({"id": [1, 2], "val": [10, 20]})
df2 = pl.DataFrame({"id": [1, 2], "extra": [100, 200]})

# simple select of all columns
print(select(df1, "*"))

# filter rows where val > 10
print(select(df1, "* : [val > 10]"))

# join df1 and df2 on id
print(select(df1, "* from A <- B on [id]", others=df2))

# group by id and sum val
print(select(df1, "* from A => groupby[id | sum()]")
```

## Contact

See the [root README](../README.md) for installation and project details.
