from typing import Callable

from .base import BaseGenerator


class WordGenerator(BaseGenerator):
    def __init__(
        self,
        num_rows: int,
        word: str,
        shuffle_callback: Callable,
        byte_array: "ByteArray",
    ):
        super().__init__(num_rows)
        self.indices = byte_array.indices
        self.BA = byte_array
        self.byte_array = byte_array.bytearray
        self.shuffle_callback = shuffle_callback
        self.word = word

    def generate_chunk(self, chunk_size: int):
        self.shuffle_callback(self.indices)
        words_chunk: list = []
        i = 0
        for _ in range(chunk_size):
            try:
                words_chunk.append(self.byte_array[self.indices[i]])
                i += 1
            except IndexError:
                i = 0
                self.shuffle_callback(self.indices)
                words_chunk.append(self.byte_array[self.indices[i]])
        yield words_chunk
