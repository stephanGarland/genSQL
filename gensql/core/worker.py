from __future__ import annotations

import random
from typing import Any, Dict, Optional


class Worker:
    def __init__(
        self,
        generator: Any,
        num_rows: int,
        seeds: Optional[Dict[str, int]] = None,
        shuffle_lib: Any = None,
    ):
        self.generator = generator
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

    def generate_column(self):
        self.generator.reset_indices()
        return list(self.generator.generate(self.num_rows))
