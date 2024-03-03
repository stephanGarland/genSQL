from collections import deque
from math import ceil

import sqlite3

from .base import BaseGenerator
from utilities import utilities

# TODO: should FirstName and LastName be merged?
class FirstName(BaseGenerator):
    def __init__(self, num_rows: int):
        super().__init__(num_rows)
        # TODO: this makes calling sample annoying
        # since you have to use self.sample(self, ...)
        self.sample = utilities.Utilities.sample
        q1 = f"""SELECT first_name FROM person_name"""
        self.cursor.execute(q1)
        self._first_names = [x[0] for x in self.cursor.fetchall()]
        self._len_first_names = len(self._first_names)
        self.conn.close()

    # TODO: the usage of self.sample may not be ideal, at least not with
    # its default sample size of 1
    def make_name(self, *args):
        for i in range(0, self.num_rows, self.chunk_size):
            self.first_names = deque()
            for _ in range(self.chunk_size):
                random_first = self.sample(self, self._first_names, self._len_first_names)
                first_name = f"{random_first}".replace("'", "''")
                self.first_names.append(first_name)
            yield self.first_names


class LastName(BaseGenerator):
    def __init__(self, num_rows: int):
        super().__init__(num_rows)
        self.sample = utilities.Utilities.sample
        q1 = f"""SELECT last_name FROM person_name"""
        self.cursor.execute(q1)
        self._last_names = [x[0] for x in self.cursor.fetchall()]
        self._len_last_names = len(self._last_names)
        self.conn.close()

    def make_name(self, *args):
        for i in range(0, self.num_rows, self.chunk_size):
            self.last_names = deque()
            for _ in range(self.chunk_size):
                random_last = self.sample(self, self._last_names, self._len_last_names)
                last_name = f"{random_last}".replace("'", "''")
                self.last_names.append(last_name)
            yield self.last_names
