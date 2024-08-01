from __future__ import annotations

from multiprocessing import shared_memory
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from array import array

MSEC_IN_SEC: int = 1000
SIZEOF_LL: int = 8


class SharedEpochManager:
    __slots__ = ["epochs", "idx", "name", "size", "shm"]

    def __init__(self, name: str, size: int):
        self.size = size
        self.shm = shared_memory.SharedMemory(
            create=True, name=name, size=size * SIZEOF_LL
        )
        self.idx = 0
        self.name = name

    def flush_epochs_to_shm(self, arr: array, size: int) -> None:
        self.shm.buf[: size * SIZEOF_LL] = arr[:size].tobytes()

    def close(self):
        self.shm.close()

    def unlink(self):
        self.shm.unlink()

    def close_and_unlink(self):
        self.shm.close()
        self.shm.unlink()
