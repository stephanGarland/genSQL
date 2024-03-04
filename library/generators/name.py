import sqlite3
from collections import deque
from math import ceil
from itertools import cycle

from utilities import utilities

from .base import BaseGenerator


# TODO: should FirstName and LastName be merged?
class FirstName(BaseGenerator):
    def __init__(self, num_rows: int, shuffle_callback):
        super().__init__(num_rows)
        self.shuffle_callback = shuffle_callback

    def make_name(self, *args):
        _first_names = [
            x
            for x in "".join([chr(x) for x in args[0].buf.tolist()]).split("\x00")
            if x
        ]
        fname_loop = cycle(_first_names)
        len_fnames_actual = len(_first_names) - 1
        counter = 0
        for i in range(0, self.num_rows, self.chunk_size):
            self.first_names = deque()
            for j in range(self.chunk_size):
                if counter == len_fnames_actual:
                    self.shuffle_callback(len_fnames_actual + 1, 16)
                    counter = 0
                    fname_loop = cycle(_first_names)
                self.first_names.append(next(fname_loop))
                counter += 1
            yield self.first_names


class LastName(BaseGenerator):
    def __init__(self, num_rows: int, shuffle_callback):
        super().__init__(num_rows)
        self.shuffle_callback = shuffle_callback

    def make_name(self, *args):
        _last_names = [
            x
            for x in "".join([chr(x) for x in args[0].buf.tolist()]).split("\x00")
            if x
        ]
        lname_loop = cycle(_last_names)
        len_lnames_actual = len(_last_names) - 1
        counter = 0
        for i in range(0, self.num_rows, self.chunk_size):
            self.last_names = deque()
            for j in range(self.chunk_size):
                if counter == len_lnames_actual:
                    self.shuffle_callback(len_lnames_actual + 1, 16)
                    counter = 0
                    lname_loop = cycle(_last_names)
                self.last_names.append(next(lname_loop))
                counter += 1
            yield self.last_names
