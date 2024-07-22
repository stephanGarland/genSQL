from __future__ import annotations

import ctypes
from datetime import datetime
from typing import Callable, Iterable

from gensql.core.constants import (DEFAULT_DATETIME_FMT,
                                   DEFAULT_GENERATE_CHUNK_SIZE)


class DateGenerator:
    def __init__(
        self,
        min_dt: str,
        max_dt: str,
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
    ):
        self.chunk_size = chunk_size
        self.lib = ctypes.CDLL("./gensql/lib/bin/xoshiro.so")
        self.lib.fill_array.argtypes = [
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_uint32,
        ]
        self.lib.fill_array.restype = ctypes.POINTER(ctypes.c_uint32)
        self.lib.free_array.argtypes = [ctypes.POINTER(ctypes.c_uint32)]
        self.lib.free_array.restype = None
        self.lib.seed_rng()
        self.min_epoch = self.convert_dtstr_to_epoch(min_dt)
        self.max_epoch = self.convert_dtstr_to_epoch(max_dt)

    def set_shuffle_callback(self, callback: dict[str, Callable]):
        pass

    def reset_indices(self):
        pass

    def convert_dtstr_to_epoch(self, dt_str: str) -> int:
        dt = datetime.strptime(dt_str, DEFAULT_DATETIME_FMT)

        return int(dt.timestamp())

    def convert_epoch_to_dtstr(self, epoch: int) -> bytearray:
        """Creates datetime strings from an epoch.
        Creates datetime stings from an epoch. This method of slicing the dt object
        with f-strings is ~22% faster than using strftime.
        """

        new_dt = datetime.utcfromtimestamp(epoch)
        yr = f"{new_dt.year:04d}"
        mo = f"{new_dt.month:02d}"
        da = f"{new_dt.day:02d}"
        hr = f"{new_dt.hour:02d}"
        mi = f"{new_dt.minute:02d}"
        se = f"{new_dt.second:02d}"
        new_dtstr = f"{yr}-{mo}-{da} {hr}:{mi}:{se}"

        return bytearray(new_dtstr.encode("utf-8"))

    def generate(self, num_rows: int) -> Iterable[list]:
        for i in range(0, num_rows, self.chunk_size):
            chunk_size = min(self.chunk_size, num_rows - i)

            yield self.generate_chunk(chunk_size)

    def generate_chunk(self, chunk_size: int):
        arr_ptr = self.lib.fill_array(chunk_size, self.min_epoch, self.max_epoch)
        numbers = [arr_ptr[i] for i in range(chunk_size)]
        self.lib.free_array(arr_ptr)

        # I'd have thought that the function call overhead for each
        # number would have an effect on speed, but no (on Mac)
        return [self.convert_epoch_to_dtstr(x) for x in numbers]
