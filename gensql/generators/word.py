from collections import deque

from .base import BaseGenerator


class Word(BaseGenerator):
    def __init__(self, num_rows: int, shm, word, shuffle_callback, buffer_data=False):
        super().__init__(num_rows)
        self.buffer_data = buffer_data
        if buffer_data:
            self.buffer = bytearray()
        self.shm = shm
        self.shuffle_callback = shuffle_callback
        self.word = word

    def _load_words_from_shm(self, word_type) -> deque:
        shm = getattr(self.shm, f"shm_{word_type}")
        words: deque = deque()
        mem_view = memoryview(shm.buf)
        for i in range(0, len(mem_view), 16):
            chunk = mem_view[i : i + 16]
            word = chunk.tobytes().rstrip(b"\x00").decode()
            if word:
                words.append(word)
        self.shm_len = len(words)
        return deque(words)

    def generate_chunk(self, *args):
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
