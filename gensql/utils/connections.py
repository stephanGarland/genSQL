import sqlite3
from contextlib import closing

from gensql.core.constants import SQLITE_DB


class SQLiteColumnGetter:
    """Retrieves an entire column from a given table in the embedded DB.

    Args:
        col_name: Valid column name existing in tbl_name.
        tbl_name: Valid table name existing in SQLITE_DB.

    Returns:
        Tuple: (longest string in the result, encoded column data)

    Raises:
        TODO
    """

    def __init__(self, col_name: str, tbl_name: str):
        self.col_name = col_name
        self.tbl_name = tbl_name
        self.conn = sqlite3.connect(SQLITE_DB)

    def get_col(self) -> tuple[int, list[str]]:
        query = (
            f"SELECT MAX(LENGTH({self.col_name})) FROM {self.tbl_name} UNION "
            f"SELECT {self.col_name} FROM {self.tbl_name}"
        )
        with closing(self.conn):
            cur = self.conn.execute(query)
            res = cur.fetchall()
        max_len, rows = res[0][0], [x[0].encode("utf-8") for x in res[1:]]

        return max_len, rows
