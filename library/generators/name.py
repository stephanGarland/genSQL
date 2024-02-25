from collections import deque
from math import ceil

import sqlite3

from library.db.sqlite.connection import SQLite
from utilities import utilities

# TODO: should FirstName and LastName be merged?
class FirstName(SQLite):
    def __init__(self, num_rows: int):
        super().__init__()
        self.chunk_size = num_rows // 10
        self.num_rows = num_rows
        self.sample = utilities.Utilities.sample
        q1 = f"""SELECT first_name FROM person_name"""
        self.cursor.execute(q1)
        self.first_names = [x[0] for x in self.cursor.fetchall()]
        self.len_first_names = len(self.first_names)
        self.conn.close()

    # TODO: the usage of self.sample may not be ideal, at least not with
    # its default sample size of 1
    def make_name(self, *args):
        for i in range(0, self.num_rows, self.chunk_size):
            first_names = deque()
            for _ in range(self.chunk_size):
                random_first = self.sample(self, self.first_names, self.len_first_names)
                first_name = f"{random_first}".replace("'", "''")
                first_names.append(first_name)
            yield first_names


class LastName(SQLite):
    def __init__(self, num_rows: int):
        super().__init__()
        self.chunk_size = num_rows // 10
        self.num_rows = num_rows
        self.sample = utilities.Utilities.sample
        sqlite = SQLite()
        q1 = f"""SELECT last_name FROM person_name"""
        self.cursor.execute(q1)
        self.last_names = [x[0] for x in self.cursor.fetchall()]
        self.len_last_names = len(self.last_names)
        self.conn.close()

    def make_name(self, *args):
        for i in range(0, self.num_rows, self.chunk_size):
            last_names = deque()
            for _ in range(self.chunk_size):
                random_last = self.sample(self, self.last_names, self.len_last_names)
                last_name = f"{random_last}".replace("'", "''")
                last_names.append(last_name)
            yield last_names

