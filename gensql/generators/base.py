from abc import ABC, abstractmethod
from math import floor


class BaseGenerator(ABC):
    def __init__(self, num_rows: int):
        self.chunk_size = min(floor(num_rows / 10), 100_000)
        self.num_rows = num_rows

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
            A list of rows for each batch
        """
        for i in range(0, self.num_rows, self.chunk_size):
            batch_size = min(self.chunk_size, self.num_rows - i)
            yield self.generate_chunk(batch_size)
