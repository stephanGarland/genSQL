import ctypes
from enum import IntEnum
from typing import Callable

from .base import BaseGenerator


class UUIDVersion(IntEnum):
    VER_4 = 4
    VER_7 = 7


class UUIDGenerator(BaseGenerator):
    def __init__(
        self, num_rows: int, shuffle_callback: Callable, uuid_version: int, **kwargs
    ):
        super().__init__(num_rows)
        self.num_rows = num_rows
        self.lib = ctypes.CDLL("./gensql/lib/bin/uuid.so")
        self.lib.fill_array.argtypes = [ctypes.c_int, ctypes.c_int]
        self.lib.fill_array.restype = ctypes.POINTER(ctypes.c_char_p)
        if uuid_version == 4:
            self.uuid_ptr = self.lib.fill_array(num_rows, UUIDVersion.VER_4)
        elif uuid_version == 7:
            self.uuid_ptr = self.lib.fill_array(num_rows, UUIDVersion.VER_7)
        else:
            raise ValueError(f"ERROR: unsupported UUID version {uuid_version}")
        self.uuids = [self.uuid_ptr[i] for i in range(num_rows)]

    def generate_chunk(self, chunk_size: int):
        for i in range(0, self.num_rows, chunk_size):
            yield self.uuids[i : i + chunk_size]
