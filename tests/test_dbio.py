import os

import polars as pl
from polars.testing import (
    assert_frame_equal,  # Do `type: ignore` to ignore the `Err` case
)

from pydian.dbio import DatabaseSession, DatabaseType

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(CURRENT_DIR, "static")


def test_database_session(simple_dataframe: pl.DataFrame) -> None:
    # Use SQLite3 to start since it's built-in stdlib
    DB_PATH = f"{STATIC_DIR}/simple_database.db"
    TABLE_NAME = "data"  # This must match the `.db` file which was pre-generated
    with DatabaseSession(DatabaseType.Sqlite3, DB_PATH) as db:
        # TODO: need to handle `None` case, e.g. should empty string
        #   ... foreshadowing for future data nuances!
        df_all = db.query("*", from_table=TABLE_NAME)
        assert_frame_equal(simple_dataframe, df_all)

        df_some = db.query("a, b", from_table=TABLE_NAME)
        assert_frame_equal(simple_dataframe[["a", "b"]], df_some)

        # df_one = db.query("a", from_table=TABLE_NAME)
        # assert_frame_equal(simple_dataframe["a"], df_one)

        df_all = db.sql_query("SELECT * FROM main;")
        assert_frame_equal(simple_dataframe, df_all)
