import ctypes
import random

from gensql.generators.email import EmailGenerator
from gensql.utils.byte_array import ByteArray


class Worker:
    def __init__(
        self,
        num_rows: int,
        seed_dict: dict,
        ColumnClass: "BaseGenerator",
        column_args: dict,
        seed: int | None = None,
    ):
        if seed is not None:
            random.seed(seed)
        else:
            seed = random.getrandbits(32)
        seed_dict.setdefault(column_args["word"], seed)
        self.seed = seed
        fast_shuffle_lib = ctypes.CDLL("./gensql/utils/libs/fast_shuffle.so")
        fast_shuffle_lib.shuf.argtypes = [
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.c_uint32,
            ctypes.c_uint32,
        ]
        self.ColumnClass = ColumnClass
        self.column_args = column_args
        self.column_args["shuffle_callback"] = self.shuffle
        self.num_rows = num_rows
        self.seed_dict = seed_dict
        self.shuf = fast_shuffle_lib.shuf

    def generate_column(self):
        column_generator = self.ColumnClass(self.num_rows, **self.column_args)
        words = []
        for chunk in column_generator.generate():
            words.extend(chunk)
        return words

    def shuffle(self, indices):
        self.shuf(indices, len(indices), self.seed)
        # self.shuf(indices, len(indices), self.seed_dict[self.column_args["word"]])
