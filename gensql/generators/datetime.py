from __future__ import annotations

from array import array
from datetime import datetime
from typing import Callable, Iterable, List

from gensql.core.constants import DEFAULT_DATETIME_FMT, DEFAULT_GENERATE_CHUNK_SIZE
from gensql.lib.wrapper.xoshiro import get_lib as xoshiro
from gensql.utils.shared_epoch import SharedEpochManager


class DateGenerator:
    def __init__(
        self,
        min_dt: str,
        max_dt: str,
        shared_epoch_manager: SharedEpochManager,
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
    ):
        self.chunk_size = chunk_size
        self.min_epoch = self.convert_dtstr_to_epoch(min_dt)
        self.max_epoch = self.convert_dtstr_to_epoch(max_dt)
        self.shared_epoch_manager = shared_epoch_manager
        self.xoshiro = xoshiro()

    def set_shuffle_callback(self, callback: dict[str, Callable]):
        pass

    def reset_indices(self):
        pass

    def convert_dtstr_to_epoch(self, dt_str: str) -> int:
        dt = datetime.strptime(dt_str, DEFAULT_DATETIME_FMT)

        return int(dt.timestamp())

    def convert_epoch_to_dtstr(self, epoch: int) -> bytes:
        """Creates datetime strings from an epoch.
        Creates datetime stings from an epoch. Slicing
        the dt object with f-strings (e.g. `yr = f"{dt.year:04d}"`)
        is ~20% faster than using strftime. Using the string
        mini-format language as below is ~40% faster.
        """

        dt = datetime.fromtimestamp(epoch)
        return b"%04d-%02d-%02d %02d:%02d:%02d" % (
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
        )

    def generate(self, num_rows: int) -> Iterable[List]:
        for i in range(0, num_rows, self.chunk_size):
            chunk_size = min(self.chunk_size, num_rows - i)

            yield self.generate_chunk(chunk_size)

    def generate_chunk(self, chunk_size: int):
        arr_ptr = self.xoshiro.fill_array(chunk_size, self.min_epoch, self.max_epoch)
        epochs = array("q", arr_ptr[:chunk_size])
        self.xoshiro.free_array(arr_ptr)
        self.shared_epoch_manager.flush_epochs_to_shm(epochs, chunk_size)

        return [(self.convert_epoch_to_dtstr(x),) for x in epochs]
