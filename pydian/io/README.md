# `io` Examples

`SomeFile` and `WorkdirSession` for local files:
```python
import os
import polars as pl
from polars.testing import assert_frame_equal
from pydian.io import SomeFile, WorkdirSession

# Define directories
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(CURRENT_DIR, "static")

# Sample data
simple_data = {"key": "value"}
simple_dataframe = pl.DataFrame({"column": [1, 2, 3]})

# Using SomeFile to read and assert file contents
with SomeFile(f"{STATIC_DIR}/simple_data.json") as simple_json_file:
    assert simple_data == simple_json_file

with SomeFile(f"{STATIC_DIR}/simple_dataframe.csv") as simple_csv_file:
    assert_frame_equal(simple_dataframe, simple_csv_file)

# Using WorkdirSession to manage multiple files
with WorkdirSession(STATIC_DIR) as wd:
    simple_json_file = wd.open("simple_data.json")
    assert simple_data == simple_json_file

    simple_csv_file = wd.open("simple_dataframe.csv")
    assert_frame_equal(simple_dataframe, simple_csv_file)

    # Regex search for files
    for obj in wd.re_search(r".*\.json"):
        assert isinstance(obj, dict)

    for obj in wd.re_search(r".*\.csv"):
        assert isinstance(obj, pl.DataFrame)
```

`DatabaseSession` for "remote" things in a database (and auto-serializes into a dataframe!):
```python
import os
import polars as pl
from pydian.io import DatabaseSession, DatabaseType

# Define paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(CURRENT_DIR, "static")
DB_PATH = os.path.join(STATIC_DIR, "simple_database.db")
TABLE_NAME = "data"

# Create a sample DataFrame
simple_dataframe = pl.DataFrame({
    "a": [1, 2, 3],
    "b": [4, 5, 6],
    "c": [7, 8, 9]
})

# Use DatabaseSession to interact with the SQLite3 database
with DatabaseSession(DatabaseType.Sqlite3, DB_PATH) as db:
    # Query all columns
    df_all = db.query("*", from_table=TABLE_NAME)
    print(df_all)

    # Query specific columns
    df_some = db.query("a, b", from_table=TABLE_NAME)
    print(df_some)

    # Execute a raw SQL query
    df_all_sql = db.sql_query("SELECT * FROM main;")
    print(df_all_sql)
```
