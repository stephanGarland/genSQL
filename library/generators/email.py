from collections import deque

from .word import Word


class Email(Word):
    def __init__(
        self, num_rows: int, shm, word, shuffle_callback, fname_instance, lname_instance
    ):
        super().__init__(num_rows, shm, word, shuffle_callback)
        self.shm = shm
        self.fname_instance = fname_instance
        self.lname_instance = lname_instance
        self.fname_index = 0
        self.lname_index = 0
        self.len_words = len(self.shm.words)
        self.words = self._load_words_from_shm("words")

    def generate_chunk(self, *args):
        emails_chunk = deque()
        # TODO: have formatting options, e.g.
        # stephan.garland@common.tld
        # s.garland@common.tld
        # sgarland@common.tld
        # garland.stephan@common.tld
        # first.last@random.tld
        self.fnames = self.fname_instance.buffer.split(b"\x00")
        self.lnames = self.lname_instance.buffer.split(b"\x00")
        for _ in range(self.chunk_size):
            try:
                domain = self.words.pop()
            except IndexError:
                self.shuffle_callback(self.len_words, 16, "words")
                self.words = self._load_words_from_shm("words")
                domain = self.words.pop()
            try:
                first_name = self.fnames[self.fname_index].decode().lower()
                last_name = self.lnames[self.lname_index].decode().lower()
            except IndexError:
                print(
                    f"fname_index: {self.fname_index} lname_index: {self.lname_index}"
                )
            self.fname_index += 1
            self.lname_index += 1
            emails_chunk.append(f"{first_name}.{last_name}@{domain}.com")
        yield emails_chunk
