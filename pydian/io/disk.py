import gc
import json
import os
import re
from typing import Any

import polars as pl


class SomeFile:
    filepath: str = ""
    data: dict | pl.DataFrame | None = None

    def __init__(self, filepath: str):
        self.filepath = filepath

    def __enter__(self):
        self.data = SomeFile.open(self.filepath)
        return self.data

    def __exit__(self, exc_type, exc_val, exc_tb):
        # No special cleanup needed
        pass

    @staticmethod
    def open(abs_fp: str) -> dict[str, Any] | list[dict[str, Any]] | pl.DataFrame:
        """
        Opens a file given the absolute filepath
        """

        def _handle_csv_import_cases(df: pl.DataFrame) -> pl.DataFrame:
            # TODO: find a nicer way to handle datatype mismatches
            # Case: for a csv col w/ all empty vals, the dtype should be `pl.Null`, not `pl.String`
            for col in df.columns:
                # Check if all values in the column are None
                if df[col].null_count() == df.height:
                    # Cast the column to Null dtype
                    df = df.with_columns(pl.col(col).cast(pl.Null))
            return df

        if abs_fp.endswith(".json"):
            with open(abs_fp, "r") as f:
                data = json.load(f)
        elif abs_fp.endswith(".ndjson"):
            with open(abs_fp, "r") as f:
                data = [json.loads(line) for line in f]
        elif abs_fp.endswith(".csv"):
            data = pl.read_csv(abs_fp, null_values="")
            data = _handle_csv_import_cases(data)
        elif abs_fp.endswith(".tsv"):
            data = pl.read_csv(abs_fp, null_values="", separator="\t")
            data = _handle_csv_import_cases(data)
        else:
            raise ValueError(f"Unsupported filename (based on file type): {abs_fp}")

        return data


class WorkdirSession:
    cd: str = ""
    _close_files_on_exit: bool = False
    _curr_open_files: dict[str, Any] | None = None
    _filenames: list[str] | None = None

    def __init__(self, workdir: str, close_files_on_exit: bool = False):
        self.cd = workdir
        self._close_files_on_exit = close_files_on_exit
        self._curr_open_files = {}
        self._filenames = []

    def __enter__(self):
        self._filenames = os.listdir(self.cd)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._close_files_on_exit:
            self._curr_open_files.clear()
        # Drop everything in the block
        gc.collect()

    def open(self, filename: str) -> dict[str, Any] | list[dict[str, Any]] | pl.DataFrame:
        fp = os.path.join(self.cd, filename)
        data = SomeFile.open(fp)
        if self._curr_open_files is not None:
            self._curr_open_files[filename] = data
        return data

    def re_search(self, regex_pattern: re.Pattern | str) -> list[Any]:
        # Look over files, and open + return all matches
        res = []
        if not isinstance(self._filenames, list):
            raise RuntimeError("`_filenames` isn't set: issue with WorkdirSession state!")
        for filename in self._filenames:
            if re.search(regex_pattern, filename):
                fp = os.path.join(self.cd, filename)
                res.append(SomeFile.open(fp))
        return res
