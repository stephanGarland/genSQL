import ctypes
from collections import defaultdict, deque


class ByteArray:
    def __init__(self, sequence: list):
        self.bytearray = [bytearray(x) for x in sequence]
        self.len_bytearray = len(self.bytearray)
        self.seeds: defaultdict = defaultdict(deque)
        self.reset_indices()

    def reset_indices(self):
        self.indices = (ctypes.c_uint32 * self.len_bytearray)(
            *(list(range(self.len_bytearray)))
        )

    def get_seed_dict(self):
        return [(k, list(v)) for k, v in self.seeds.items()]
