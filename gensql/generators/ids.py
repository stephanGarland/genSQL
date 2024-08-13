from __future__ import annotations

from array import array
from typing import Callable, Iterable, List, Tuple

from gensql.core.constants import (
    DEFAULT_GENERATE_CHUNK_SIZE,
    ID_FORMATS,
)
from gensql.lib.wrapper.fast_div import get_lib as fast_div
from gensql.lib.wrapper.xoshiro import get_lib as xoshiro


class NumGenerator:
    def __init__(
        self,
        min_num: str,
        max_num: str,
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
    ):
        self.chunk_size = chunk_size
        self.fast_div = fast_div()
        self.min_num = min_num
        self.max_num = max_num
        self.xoshiro = xoshiro()

    def set_shuffle_callback(self, callback: dict[str, Callable]):
        pass

    def reset_indices(self):
        pass

    def format_id(self, id_type: str, num: int) -> bytes:
        if id_type in ID_FORMATS:
            fmt, divisors, moduli = ID_FORMATS[id_type]
        elif id_type == "custom":
            raise ValueError("Custom format not provided")
        else:
            raise ValueError(f"Unsupported ID type: {id_type}")

        return self._make_formatted(fmt, num, divisors, moduli)

    def _make_formatted(
        self, fmt: bytes, num: int, divisors: Tuple[int, ...], moduli: Tuple[int, ...]
    ) -> bytes:
        parts: List[int] = []
        for i, div in enumerate(divisors):
            precomputed = self.fast_div.precompute_mod_u32(div)
            quotient = self.fast_div.fastdiv_u32(num, precomputed)
            parts.append(quotient)
            if i < len(moduli):
                num = self.fast_div.fastmod_u32(num, precomputed, div)

        parts.append(num)
        return fmt % tuple(parts)

    def generate_formatted_ids(
        self, id_type: str, start: int, end: int, count: int
    ) -> List[bytes]:
        import random

        if id_type == "PHONE_US":
            start = max(start, 1000000000)
            end = min(end, 9999999999)

        return [
            self.format_id(id_type, random.randint(start, end)) for _ in range(count)
        ]

    def debug_generate_formatted_phones(self, count: int) -> list[str]:
        import random

        phones = []
        for _ in range(count):
            # Generate a random 10-digit number
            num = random.randint(1000000000, 9999999999)

            # Format the number
            area_code = num // 10000000
            prefix = (num // 10000) % 1000
            line_number = num % 10000

            formatted_phone = f"{area_code:03d}-{prefix:03d}-{line_number:04d}"
            phones.append(formatted_phone)

        return phones

    def generate(self, num_rows: int) -> Iterable[List]:
        for i in range(0, num_rows, self.chunk_size):
            chunk_size = min(self.chunk_size, num_rows - i)

            yield self.generate_chunk(chunk_size)

    def generate_chunk(self, chunk_size: int):
        arr_ptr = self.xoshiro.fill_array(chunk_size, self.min_num, self.max_num)
        nums = array("q", arr_ptr[:chunk_size])
        self.xoshiro.free_array(arr_ptr)

        return list(nums)
