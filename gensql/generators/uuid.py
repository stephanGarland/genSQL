from __future__ import annotations

import ctypes
from enum import Enum
from typing import Callable, Dict, Iterable, List

from gensql.core.constants import DEFAULT_GENERATE_CHUNK_SIZE
from gensql.lib.wrapper.uuid import get_lib as uuidgen
from gensql.utils.shared_epoch import SharedEpochManager


class UUIDVersion(Enum):
    VER_4 = 4
    VER_7 = 7


class UUIDGenerator:
    def __init__(
        self,
        uuid_version: UUIDVersion,
        shared_epoch_manager: SharedEpochManager,
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
    ):
        self.chunk_size = chunk_size
        self.shared_epoch_manager = shared_epoch_manager
        self.uuidgen = uuidgen()
        self.uuid_version = uuid_version

    def set_shuffle_callback(self, callback: Dict[str, Callable]):
        pass

    def reset_indices(self):
        pass

    def generate(self, num_rows: int) -> Iterable[list]:
        for i in range(0, num_rows, self.chunk_size):
            chunk_size = min(self.chunk_size, num_rows - i)

            yield self.generate_chunk(chunk_size)

    def generate_chunk(self, chunk_size: int):
        epoch_count = self.shared_epoch_manager.size
        arr_ptr = self.uuidgen.fill_array(
            chunk_size, self.uuid_version.value, b"/datetimes", epoch_count
        )

        uuid_array = ctypes.cast(
            arr_ptr, ctypes.POINTER(ctypes.c_char * (37 * chunk_size))
        )
        uuids: List[List[bytes]] = [
            (uuid_array.contents[i * 37 : (i + 1) * 37],) for i in range(chunk_size)
        ]

        return uuids
