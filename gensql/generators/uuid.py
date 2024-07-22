from __future__ import annotations

import ctypes
from enum import Enum
from typing import Callable, Iterable

from gensql.core.constants import DEFAULT_GENERATE_CHUNK_SIZE


class UUIDVersion(Enum):
    VER_4 = 4
    VER_7 = 7


class UUIDGenerator:
    def __init__(
        self, uuid_version: UUIDVersion, chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE
    ):
        self.chunk_size = chunk_size
        self.lib = ctypes.CDLL("./gensql/lib/bin/uuid.so")
        self.lib.fill_array.argtypes = [ctypes.c_int, ctypes.c_int]
        self.lib.fill_array.restype = ctypes.POINTER(ctypes.c_char_p)
        self.lib.free_array.argtypes = [ctypes.POINTER(ctypes.c_char_p)]
        self.lib.free_array.restype = None
        self.uuid_version = uuid_version

    def set_shuffle_callback(self, callback: dict[str, Callable]):
        pass

    def reset_indices(self):
        pass

    def generate(self, num_rows: int) -> Iterable[list]:
        for i in range(0, num_rows, self.chunk_size):
            chunk_size = min(self.chunk_size, num_rows - i)

            yield self.generate_chunk(chunk_size)

    def generate_chunk(self, chunk_size: int):
        arr_ptr = self.lib.fill_array(chunk_size, self.uuid_version.value)
        uuids = [arr_ptr[i] for i in range(chunk_size)]
        self.lib.free_array(arr_ptr)

        return uuids
