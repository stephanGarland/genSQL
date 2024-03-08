from collections import deque
from math import ceil

from gensql.utils import constants as const
from gensql.utils import utilities

from .base import BaseGenerator


class SSN(BaseGenerator):
    def __init__(self, num_rows: int):
        super().__init__(num_rows)
        self.allocator = utilities.Allocator
        self._prepare_ssn_allocator()

    def _prepare_ssn_allocator(self):
        # TODO: replace these with shuffled shms
        deques_needed = ceil(self.num_rows / (const.MAX_SSN - const.MIN_SSN) * 2)
        self.random_ssn = self.allocator(
            const.MIN_SSN, const.MAX_SSN, ranged_arr=True, shuffle=True
        )
        if deques_needed > 1:
            for _ in range(deques_needed):
                self.random_ssn.ids += self.allocator(
                    const.MIN_SSN,
                    const.MAX_SSN,
                    ranged_arr=True,
                    shuffle=True,
                ).ids

    def generate_chunk(self, *args):
        ssns_chunk = deque()
        for _ in range(self.chunk_size):
            ssn_val_1 = self.random_ssn.allocate()
            ssn_val_2 = self.random_ssn.allocate()
            ssn_str = f"{ssn_val_1}{ssn_val_2}"
            ssn = const.SSNS(ssn_str)
            ssns_chunk.append(ssn)

        yield ssns_chunk
