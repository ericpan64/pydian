import os
from typing import Any

import polars as pl
import pytest
from polars.testing import (
    assert_frame_equal,  # Do `type: ignore` to ignore the `Err` case
)

from pydian.dio import SomeFile, WorkdirSession

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(CURRENT_DIR, "static")


def test_somefile(simple_data: dict[str, Any], simple_dataframe: pl.DataFrame) -> None:
    with SomeFile(f"{STATIC_DIR}/simple_data.json") as simple_json_file:
        assert simple_data == simple_json_file.data

    simple_data_10 = [simple_data] * 10
    with SomeFile(f"{STATIC_DIR}/simple_data_10.ndjson") as simple_ndjson_file:
        assert simple_data_10 == simple_ndjson_file.data

    with SomeFile(f"{STATIC_DIR}/simple_dataframe.csv") as simple_csv_file:
        assert_frame_equal(simple_dataframe, simple_csv_file.data)

    with SomeFile(f"{STATIC_DIR}/simple_dataframe.tsv") as simple_tsv_file:
        assert_frame_equal(simple_dataframe, simple_tsv_file.data)


def test_somefile_open(simple_data: dict[str, Any], simple_dataframe: pl.DataFrame) -> None:
    # Happy path!
    assert simple_data == SomeFile.grab(f"{STATIC_DIR}/simple_data.json")
    assert [simple_data] * 10 == SomeFile.grab(f"{STATIC_DIR}/simple_data_10.ndjson")
    assert_frame_equal(simple_dataframe, SomeFile.grab(f"{STATIC_DIR}/simple_dataframe.csv"))  # type: ignore
    assert_frame_equal(simple_dataframe, SomeFile.grab(f"{STATIC_DIR}/simple_dataframe.tsv"))  # type: ignore
    assert isinstance(SomeFile.grab(f"{STATIC_DIR}/example_str.txt"), str)

    # Error case
    with pytest.raises(ValueError):
        SomeFile.grab(f"{STATIC_DIR}/Non-Existant-File")

    with pytest.raises(FileNotFoundError):
        SomeFile.grab(f"{STATIC_DIR}/Non-Existant-File.txt")

    with pytest.raises(ValueError):
        SomeFile.grab(f"{STATIC_DIR}/example_unsupported_ext.pid")


def test_workdir_session(simple_data: dict[str, Any], simple_dataframe: pl.DataFrame) -> None:
    # Regular session
    with WorkdirSession(STATIC_DIR) as wd:
        example_str = wd.open("example_str.txt")
        assert isinstance(example_str, str)

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


def test_workdir_list_files(simple_data: dict[str, Any], simple_dataframe: pl.DataFrame) -> None:
    with WorkdirSession(STATIC_DIR) as wd:
        # Try regex search
        for obj in wd.re_search(r".*\.json"):
            assert isinstance(obj, dict)

        for obj in wd.re_search(r".*\.ndjson"):
            assert isinstance(obj, list)
            if len(obj) > 0:
                assert isinstance(obj[0], dict)

        for obj in wd.re_search(r".*\.csv"):
            assert isinstance(obj, pl.DataFrame)

        for obj in wd.re_search(r".*\.tsv"):
            assert isinstance(obj, pl.DataFrame)

        # On a failed search: return an empty list.
        # User can handle this however they want!
        count = 0
        for obj in wd.re_search(r"some_filename_not_there\.woah"):
            count += 1
        assert count == 0
