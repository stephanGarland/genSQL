from collections import deque

from gensql.utils.shmem import ShMemList

from .base import BaseGenerator


class Word(BaseGenerator):
    def __init__(self, num_rows: int, word: str, shuffle_callback):
        super().__init__(num_rows)
        self.shuffle_callback = shuffle_callback
        self.shml = ShMemList(shmem_name=word)
        self.word = word

    def load_words_from_shm(self) -> None:
        self.words: deque = deque()
        for i in self.shml.indices:
            word = self.shml.shmem[i]
            if word:
                self.words.append(word)

    def generate_chunk(self, chunk_size: int):
        self.shuffle_callback(self.shml.indices)
        self.load_words_from_shm()
        words_chunk: deque = deque()
        for _ in range(chunk_size):
            try:
                word = self.words.popleft()
                words_chunk.append(word)
            except IndexError:
                self.load_words_from_shm()
                self.shuffle_callback(self.shml.indices)
                word = self.words.popleft()
                words_chunk.append(word)
        yield words_chunk

    def _generate_chunk(self, *args):
        self.words = self._load_words_from_shm(self.word)
        words_chunk = deque()
        for _ in range(self.chunk_size):
            try:
                word = self.words.popleft()
                words_chunk.append(word)
                if self.buffer_data:
                    self.buffer += word.encode() + b"\x00"
            except IndexError:
                self.shuffle_callback(self.shm_len, 16, self.word)
                self.words = self._load_words_from_shm(self.word)
                word = self.words.popleft()
                if self.buffer_data:
                    self.buffer += word.encode() + b"\x00"
                words_chunk.append(word)
        yield words_chunk
