from __future__ import annotations

import ctypes
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional

from gensql.core.constants import (
    DEFAULT_GENERATE_CHUNK_SIZE,
    PHONE_AREA_CODE_STR_LEN_US,
    PHONE_FMT_STRFMT_US,
    PHONE_MAX,
    PHONE_MIN,
    PHONE_SEP_STR_LEN_US,
    PHONE_STR_LEN_US,
)
from gensql.lib.wrapper.phone import get_lib as phonegen
from gensql.lib.wrapper.xoshiro import get_lib as xoshiro

if TYPE_CHECKING:
    from gensql.utils.shuffleable_bytes import ShuffleableBytes


class PhoneGenerator:
    def __init__(
        self,
        byte_list: ShuffleableBytes,
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
        generator_name: str = "default",
        generate_area_code: bool = False,
    ):
        self.byte_list = byte_list
        self.chunk_size = chunk_size
        self.generate_area_code = generate_area_code
        self.shuffle_callback: Optional[Dict[str, Callable]] = None
        self.generator_name = generator_name
        self.phonegen = phonegen()
        self.xoshiro = xoshiro()

    def set_shuffle_callback(self, callback: Dict[str, Callable]):
        self.shuffle_callback = callback

    def reset_indices(self):
        self.byte_list.reset_indices()

    def generate(self, num_rows: int) -> Iterable[list]:
        for i in range(0, num_rows, self.chunk_size):
            chunk_size = min(self.chunk_size, num_rows - i)

            yield self.generate_chunk(chunk_size)

    def generate_chunk(self, chunk_size: int):
        if not self.generate_area_code:
            phone_str_len = PHONE_STR_LEN_US + PHONE_SEP_STR_LEN_US
        else:
            phone_str_len = (
                PHONE_STR_LEN_US + PHONE_AREA_CODE_STR_LEN_US + PHONE_SEP_STR_LEN_US
            )
        nums_arr_ptr = self.xoshiro.fill_array(chunk_size, PHONE_MIN, PHONE_MAX)
        phone_arr_ptr = self.phonegen.fill_array(
            nums_arr_ptr, chunk_size, self.generate_area_code
        )
        phone_array = ctypes.cast(
            phone_arr_ptr,
            ctypes.POINTER(ctypes.c_char * (phone_str_len * chunk_size)),
        )
        # since the array is filled with snprintf, there's always a NUL
        # terminator; to avoid having to decode, skip it when slicing with -1
        phones: List[List[bytes]] = [
            phone_array.contents[i * phone_str_len : ((i + 1) * phone_str_len) - 1]
            for i in range(chunk_size)
        ]

        if not self.generate_area_code:
            if self.shuffle_callback:
                self.shuffle_callback[self.generator_name](self.byte_list.indices)
            areacode_chunk: List[bytes] = []
            i = 0
            for _ in range(chunk_size):
                try:
                    areacode = self.byte_list.bytelist[self.byte_list.indices[i]]
                    areacode_chunk.append(areacode)
                    i += 1
                except IndexError as exc:
                    i = 0
                    if self.shuffle_callback:
                        self.shuffle_callback[self.generator_name](
                            self.byte_list.indices
                        )
                    else:
                        raise exc
                    areacode = self.byte_list.bytelist[self.byte_list.indices[i]]
                    areacode_chunk.append(areacode)

        self.xoshiro.free_array(nums_arr_ptr)
        self.phonegen.free_array(phone_arr_ptr)
        if not self.generate_area_code:
            return [
                (PHONE_FMT_STRFMT_US % (_area, _phone),)
                for _area, _phone in zip(areacode_chunk, phones)
            ]
        else:
            return phones
