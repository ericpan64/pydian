import gc
import json
import os
from typing import Any

import polars as pl


class WorkdirSession:
    def __init__(self, workdir: str, close_files_on_exit: bool = False):
        self.workdir = workdir
        self.close_files_on_exit = close_files_on_exit
        self.open_files: dict[str, Any] = {}
        self.files: list[str] = []

    def __enter__(self):
        self.files = os.listdir(self.workdir)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.close_files_on_exit:
            self.open_files.clear()
        # Drop everything in the block
        gc.collect()

    def open(self, filename: str) -> dict[str, Any] | list[dict[str, Any]] | pl.DataFrame:
        file_path = os.path.join(self.workdir, filename)
        if filename.endswith(".json"):
            with open(file_path, "r") as f:
                data = json.load(f)
        elif filename.endswith(".ndjson"):
            with open(file_path, "r") as f:
                data = [json.loads(line) for line in f]
        elif filename.endswith(".csv"):
            data = pl.read_csv(file_path, null_values="")
            data = self._handle_csv_import_cases(data)
        elif filename.endswith(".tsv"):
            data = pl.read_csv(file_path, null_values="", separator="\t")
            data = self._handle_csv_import_cases(data)
        else:
            raise ValueError(f"Unsupported filename (based on file type): {filename}")

        self.open_files[filename] = data
        return data

    def _handle_csv_import_cases(self, df: pl.DataFrame) -> pl.DataFrame:
        # TODO: find a nicer way to handle datatype mismatches
        # Case: for a csv col w/ all empty vals, the dtype should be `pl.Null`, not `pl.String`
        for col in df.columns:
            # Check if all values in the column are None
            if df[col].null_count() == df.height:
                # Cast the column to Null dtype
                df = df.with_columns(pl.col(col).cast(pl.Null))
        return df
