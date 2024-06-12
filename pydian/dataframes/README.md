# `dataframes` Examples

The goal with this module is to do the "optimal" things under-the-hood for the corresponding dataframe library (right now: [polars](https://pola.rs/)).

`select` is the main driver to make grabbing data easier:

```python
import polars as pl
from pydian.dataframes import select
import pydian.partials as p

# Create a sample dataframe
df = pl.DataFrame({
    "a": [1, 2, 3],
    "b": [4, 5, 6],
    "c": [7, 8, 9]
})

# Select a single column
selected_df = select(df, "a")
print(selected_df)

# Select multiple columns
selected_df = select(df, "a, b")
print(selected_df)

# Select all columns
selected_df = select(df, "*")
print(selected_df)

# Handle non-existent columns
result = select(df, "non_existent_col", apply=p.equals("thing"))
print(result)  # Should be an instance of Err
```

`left_join` and `inner_join` can be used to join dataframes:

```python
from pydian.dataframes import left_join

# Create sample dataframes
df_left = pl.DataFrame({
    "a": [1, 2, 3],
    "b": ["x", "y", "z"]
})

df_right = pl.DataFrame({
    "a": [2, 3, 4],
    "c": ["foo", "bar", "baz"]
})

# Perform a left join
result = left_join(df_left, df_right, on="a")
print(result)

# Perform an inner join
result = inner_join(df1, df2, on="b")
print(result)
```

The functions return an `Err` instance when an operation fails, such as when trying to join on non-existent columns. Deferring this gives an extra (and friendlier) opportunity to wrangle more common, expected errors when exploring data rather than unexpected exceptions!

```python
from result import Err

# Example of handling an error
result = select(df, "non_existent_col")
if isinstance(result, Err):
    print("An error occurred:", result)
```
