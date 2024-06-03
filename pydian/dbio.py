import sqlite3
from enum import Enum

import polars as pl


class DatabaseType(Enum):
    Sqlite3 = "sqlite3"
    # Add other database types as needed


class DatabaseSession:
    db_type: DatabaseType | None = None

    def __init__(self, db_type: DatabaseType, connection_string: str):
        self.db_type = db_type
        self.connection_string = connection_string
        self.connection = None

    def __enter__(self):
        if self.db_type == DatabaseType.Sqlite3:
            self.connection = sqlite3.connect(self.connection_string)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

    def query(self, columns: str, from_table: str) -> pl.DataFrame:
        query = f"SELECT {columns} FROM {from_table};"
        return self.sql_query(query)

    def sql_query(self, query: str) -> pl.DataFrame:
        cursor = self.connection.cursor()  # type: ignore
        cursor.execute(query)
        rows = cursor.fetchall()
        # TODO: Need to preserve info about dtype
        columns = [description[0] for description in cursor.description]
        cursor.close()
        return pl.DataFrame(rows, schema=columns)


# Example usage:
# with DatabaseSession(DatabaseType.Sqlite3, "path/to/database.db") as db:
#     df = db.query("*", from_table="main")
#     print(df)
