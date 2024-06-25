from typing import Callable

from .base import BaseGenerator


class CombinedGenerator(BaseGenerator):
    def __init__(
        self,
        num_rows: int,
        generators: list[BaseGenerator],
        combine_func: Callable | None = None,
    ):
        super().__init__(num_rows)
        self.generators = generators
        self.combine_func = combine_func or self._default_combine_func

    def _default_combine_func(self, *chunks):
        return zip(*chunks)

    def generate_chunk(self, chunk_size: int):
        chunks = [g.generate_chunk(chunk_size) for g in self.generators]
        for combined_chunk in self.combine_func(*chunks):
            yield combined_chunk
