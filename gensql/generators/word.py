from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional

from gensql.core.constants import DEFAULT_GENERATE_CHUNK_SIZE

if TYPE_CHECKING:
    from gensql.utils.shuffleable_bytes import ShuffleableBytes


class WordGenerator:
    def __init__(
        self,
        byte_list: ShuffleableBytes,
        is_lower: bool = False,
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
        generator_name: str = "default",
    ):
        self.byte_list = byte_list
        self.chunk_size = chunk_size
        self.is_lower = is_lower
        self.shuffle_callback: Optional[Dict[str, Callable]] = None
        self.generator_name = generator_name

    def set_shuffle_callback(self, callback: Dict[str, Callable]):
        self.shuffle_callback = callback

    def generate(self, num_rows: int) -> Iterable[List[bytes]]:
        for i in range(0, num_rows, self.chunk_size):
            chunk_size = min(self.chunk_size, num_rows - i)
            yield self.generate_chunk(chunk_size)

    def generate_chunk(self, chunk_size: int) -> List[bytes]:
        if self.shuffle_callback:
            self.shuffle_callback[self.generator_name](self.byte_list.indices)
        words_chunk: List[bytes] = []
        i = 0
        for _ in range(chunk_size):
            try:
                word = self.byte_list.bytelist[self.byte_list.indices[i]]
                words_chunk.append(word.lower() if self.is_lower else word)
                i += 1
            except IndexError as exc:
                i = 0
                if self.shuffle_callback:
                    self.shuffle_callback[self.generator_name](self.byte_list.indices)
                else:
                    raise exc
                word = self.byte_list.bytelist[self.byte_list.indices[i]]
                words_chunk.append(word.lower() if self.is_lower else word)
        if self.generator_name == "fname" or self.generator_name == "lname":
            return [(x,) for x in words_chunk]
        else:
            return words_chunk

    def reset_indices(self):
        self.byte_list.reset_indices()
