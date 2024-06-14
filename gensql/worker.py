import ctypes
import random


class Worker:
    def __init__(
        self,
        num_rows: int,
        seed_dict: dict,
        WordClass,
        word: str,
        seed: int | None = None,
    ):
        if seed is not None:
            random.seed(seed)
        else:
            seed = random.getrandbits(32)
        seed_dict.setdefault(word, seed)

        fast_shuffle_lib = ctypes.CDLL("./gensql/utils/libs/fast_shuffle.so")
        fast_shuffle_lib.shuf.argtypes = [
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.c_uint32,
            ctypes.c_uint32,
        ]
        self.num_rows = num_rows
        self.shuf = fast_shuffle_lib.shuf
        self.seed_dict = seed_dict
        self.word = word
        self.WordClass = WordClass

    def generate_words(self) -> list:
        words = []
        word_class = self.WordClass(self.num_rows, self.word, self.shuffle)
        for chunk in word_class.generate_chunk(self.num_rows):
            words.extend(chunk)
        return words

    def shuffle(self, indices):
        self.shuf(indices, len(indices), self.seed_dict[self.word])
