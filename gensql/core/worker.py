from __future__ import annotations

import random
from typing import Any, Dict, Generator, List, Optional

from gensql.core.constants import DEFAULT_GENERATE_CHUNK_SIZE as CHUNK_SIZE


class Worker:
    def __init__(
        self,
        generator: Any,
        num_rows: int,
        chunk_size: int = CHUNK_SIZE,
        seeds: Optional[Dict[str, int]] = None,
        shuffle_lib: Any = None,
    ):
        self.generator = generator
        self.chunk_size = chunk_size
        self.num_rows = num_rows
        self.seeds = seeds or {"default": random.getrandbits(32)}
        self.shuffle_lib = shuffle_lib
        self.shufflers = self._create_shufflers() if self.shuffle_lib else None

        if self.shufflers:
            self.generator.set_shuffle_callback(self.shufflers)

    def _create_shufflers(self):
        return {
            name: lambda indices, seed=seed: self.shuffle_lib.shuf(
                indices, len(indices), seed
            )
            for name, seed in self.seeds.items()
        }

    def generate_column(self) -> Generator[List[Any], None, None]:
        self.generator.reset_indices()
        generated = 0
        while generated < self.num_rows:
            chunk_size = min(self.chunk_size, self.num_rows - generated)
            yield from self.generator.generate(chunk_size)
            generated += chunk_size
