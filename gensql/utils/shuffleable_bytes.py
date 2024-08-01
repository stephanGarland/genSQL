import ctypes
from collections import defaultdict, deque
from typing import List


class ShuffleableBytes:
    """Creates a list of bytes.
    Creates a list of bytes, and also encapsulates seeds and shuffle functions."
    """

    def __init__(self, sequence: List):
        self.bytelist: List[bytes] = sequence
        self.len_bytelist: int = len(self.bytelist)
        self.seeds: defaultdict = defaultdict(deque)
        self.reset_indices()

    def reset_indices(self):
        self.indices = (ctypes.c_uint32 * self.len_bytelist)(
            *(list(range(self.len_bytelist)))
        )

    def get_seed_dict(self):
        return [(k, list(v)) for k, v in self.seeds.items()]
