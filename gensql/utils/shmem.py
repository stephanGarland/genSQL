import ctypes
from multiprocessing.shared_memory import ShareableList


class ShMemList:
    __slots__ = ["indices", "shmem", "len_shmem"]

    def __init__(self, shmem_name: str, create: bool = False, sequence: list = list()):
        if create:
            self.shmem = ShareableList(sequence=sequence, name=shmem_name)
        else:
            self.shmem = ShareableList(name=shmem_name)
        self.len_shmem = len(self.shmem)
        self.indices = (ctypes.c_uint32 * self.len_shmem)(
            *(list(range(self.len_shmem)))
        )

    def close(self):
        self.shmem.shm.close()

    def unlink(self):
        self.shmem.shm.unlink()
