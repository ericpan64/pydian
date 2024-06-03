import os
from typing import Any

import polars as pl
from polars.testing import (
    assert_frame_equal,  # Do `type: ignore` to ignore the `Err` case
)

from pydian.dio import SomeFile, WorkdirSession

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(CURRENT_DIR, "static")


def test_somefile(simple_data: dict[str, Any], simple_dataframe: pl.DataFrame) -> None:
    with SomeFile(f"{STATIC_DIR}/simple_data.json") as simple_json_file:
        assert simple_data == simple_json_file

    simple_data_10 = [simple_data] * 10
    with SomeFile(f"{STATIC_DIR}/simple_data_10.ndjson") as simple_ndjson_file:
        assert simple_data_10 == simple_ndjson_file

    with SomeFile(f"{STATIC_DIR}/simple_dataframe.csv") as simple_csv_file:
        assert_frame_equal(simple_dataframe, simple_csv_file)

    with SomeFile(f"{STATIC_DIR}/simple_dataframe.tsv") as simple_tsv_file:
        assert_frame_equal(simple_dataframe, simple_tsv_file)


def test_workdir_session(simple_data: dict[str, Any], simple_dataframe: pl.DataFrame) -> None:
    # Regular session
    with WorkdirSession(STATIC_DIR) as wd:
        simple_json_file: dict = wd.open("simple_data.json")
        assert simple_data == simple_json_file

        simple_data_10 = [simple_data] * 10
        simple_ndjson_file: list[dict] = wd.open("simple_data_10.ndjson")
        assert simple_data_10 == simple_ndjson_file

        simple_csv_file: pl.DataFrame = wd.open("simple_dataframe.csv")
        assert_frame_equal(simple_dataframe, simple_csv_file)

        simple_tsv_file: pl.DataFrame = wd.open("simple_dataframe.tsv")
        assert_frame_equal(simple_dataframe, simple_tsv_file)

    # Files don't close when session ends (i.e. `simple_json_file` still exists in scope)
    assert simple_data == simple_json_file
    assert simple_data_10 == simple_ndjson_file
    assert_frame_equal(simple_dataframe, simple_csv_file)
    assert_frame_equal(simple_dataframe, simple_tsv_file)

    # # Session where files close at the end
    # with WorkdirSession(STATIC_DIR, close_files_on_exit=True) as wd:
    #     temp_json_file = wd.open("simple_data.json")

    # assert len(temp_json_file) == 0
