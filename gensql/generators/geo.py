from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, Generator, Iterable, List, Optional

from gensql.core.constants import DEFAULT_GENERATE_CHUNK_SIZE

if TYPE_CHECKING:
    from gensql.utils.shuffleable_bytes import ShuffleableBytes


class GeoGenerator:
    def __init__(
        self,
        byte_list: ShuffleableBytes,
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
        generator_name: str = "default",
    ):
        self.byte_list = byte_list
        self.chunk_size = chunk_size
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
        geo_chunk: List[bytes] = []
        i = 0
        for _ in range(chunk_size):
            try:
                geo = self.byte_list.bytelist[self.byte_list.indices[i]]
                geo_chunk.append(geo)
                i += 1
            except IndexError as exc:
                i = 0
                if self.shuffle_callback:
                    self.shuffle_callback[self.generator_name](self.byte_list.indices)
                else:
                    raise exc
                geo = self.byte_list.bytelist[self.byte_list.indices[i]]
                geo_chunk.append(geo)
        return geo_chunk

    def reset_indices(self):
        self.byte_list.reset_indices()


class CompositeGeoGenerator:
    def __init__(
        self,
        byte_lists: List[ShuffleableBytes],
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
        generator_name: str = "composite_geo",
    ):
        self.generators = [
            GeoGenerator(ba, chunk_size, f"{generator_name}_{i}")
            for i, ba in enumerate(byte_lists)
        ]
        self.chunk_size = chunk_size
        self.generator_name = generator_name

    def set_shuffle_callback(self, callback: Dict[str, Callable]):
        for gen in self.generators:
            gen.set_shuffle_callback(callback)

    def generate(self, num_rows: int) -> Generator:
        generator_iterables = [gen.generate(num_rows) for gen in self.generators]
        for chunks in zip(*generator_iterables):
            yield list(zip(*chunks))

    def reset_indices(self):
        for gen in self.generators:
            gen.reset_indices()
