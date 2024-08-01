import sqlite3
from contextlib import closing
from typing import Dict, List, Tuple, Union

from gensql.core.constants import SQLITE_DB


class SQLiteColumnGetter:
    def __init__(self):
        self.conn = sqlite3.connect(SQLITE_DB)

    def get_columns(
        self, columns: Union[str, List[str]], table: str, join_clause: str = ""
    ) -> Dict[str, List[str]]:
        """Retrieves specified columns from a given table in the embedded DB.

        Args:
            columns: A string or list of strings representing column names.
            table: Valid table name existing in the database.
            join_clause: Optional JOIN clause for the query.

        Returns:
            Dict: A dictionary where keys are column names and values are lists of byte strings.

        Raises:
            sqlite3.Error: If there's an issue with the database operation."""

        if isinstance(columns, str):
            columns = [columns]

        column_str = ", ".join(columns)

        query = f"SELECT {column_str} FROM {table} {join_clause}"

        try:
            with closing(self.conn.cursor()) as cur:
                cur.execute(query)
                results = cur.fetchall()

            data_dict: Dict = {col: [] for col in columns}
            for i, col in enumerate(columns):
                data_dict[col] = [str(row[i]).encode("utf-8") for row in results]
            return data_dict

        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            raise

    def close(self):
        self.conn.close()
