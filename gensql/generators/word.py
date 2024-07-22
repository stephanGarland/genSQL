from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Iterable, Optional

from gensql.core.constants import DEFAULT_GENERATE_CHUNK_SIZE

if TYPE_CHECKING:
    from gensql.utils.shuffleable_byte_array import ShuffleableByteArray


class WordGenerator:
    def __init__(
        self,
        byte_array: ShuffleableByteArray,
        is_lower: bool = False,
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
        generator_name: str = "default",
    ):
        self.byte_array = byte_array
        self.chunk_size = chunk_size
        self.is_lower = is_lower
        self.shuffle_callback: Optional[dict[str, Callable]] = None
        self.generator_name = generator_name

    def set_shuffle_callback(self, callback: dict[str, Callable]):
        self.shuffle_callback = callback

    def generate(self, num_rows: int) -> Iterable[list[bytearray]]:
        for i in range(0, num_rows, self.chunk_size):
            chunk_size = min(self.chunk_size, num_rows - i)
            yield self.generate_chunk(chunk_size)

    def generate_chunk(self, chunk_size: int) -> list[bytearray]:
        if self.shuffle_callback:
            self.shuffle_callback[self.generator_name](self.byte_array.indices)
        words_chunk: list[bytearray] = []
        i = 0
        for _ in range(chunk_size):
            try:
                word = self.byte_array.bytearray[self.byte_array.indices[i]]
                words_chunk.append(word.lower() if self.is_lower else word)
                i += 1
            except IndexError as exc:
                i = 0
                if self.shuffle_callback:
                    self.shuffle_callback[self.generator_name](self.byte_array.indices)
                else:
                    raise exc
                word = self.byte_array.bytearray[self.byte_array.indices[i]]
                words_chunk.append(word.lower() if self.is_lower else word)
        return words_chunk

    def reset_indices(self):
        self.byte_array.reset_indices()
