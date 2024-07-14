import ctypes
from typing import Callable

from .base import BaseGenerator


class UUIDGenerator(BaseGenerator):
    def __init__(
        self, num_rows: int, shuffle_callback: Callable, uuid_v4: bool = True, uuid_v7: bool = False, **kwargs
    ):
        super().__init__(num_rows)
        self.num_rows = num_rows
        self.lib = ctypes.CDLL("gensql/utils/libs/uuid.so")
        self.lib.fill_array.argtypes = [ctypes.c_int, ctypes.c_bool, ctypes.c_bool]
        self.lib.fill_array.restype = ctypes.POINTER(ctypes.c_char_p)
        if uuid_v4:
            self.uuid_ptr = self.lib.fill_array(num_rows, True, False)
        elif uuid_v7:
            self.uuid_ptr = self.lib.fill_array(num_rows, False, True)
        else:
            self.uuid_ptr = self.lib.fill_array(num_rows, False, False)
        self.uuids = [self.uuid_ptr[i] for i in range(num_rows)]

    def generate_chunk(self, chunk_size: int):
        for i in range(0, self.num_rows, chunk_size):
            yield self.uuids[i : i + chunk_size]
