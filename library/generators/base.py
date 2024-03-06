from abc import ABC, abstractmethod
from collections import deque
from math import floor

from library.db.sqlite.connection import SQLiteMixin

class BaseGenerator(ABC, SQLiteMixin):
    def __init__(self, num_rows: int, shuffle_callback = None):
        super().__init__()
        self.chunk_size = min(floor(num_rows / 10), 100_000)
        self.num_rows = num_rows
        self.shuffle_callback = shuffle_callback

    @abstractmethod
    def generate_chunk(self, chunk_size: int):
        """
        Subclasses must override this method to generate chunks.
        Args:
            chunk_size: number of rows to generate at once

        Returns:
            An iterable of rows
        """
        pass


    def generate(self, *args):
        """
        Generates rows in batches via generate_chunk()
        Args:
            None
        Returns:
            A deque of rows for each batch
        """
        for i in range(0, self.num_rows, self.chunk_size):
            # TODO: remove?
            batch_size = min(self.chunk_size, self.num_rows - i)
            entries = deque(self.generate_chunk(self.chunk_size))
            yield entries
