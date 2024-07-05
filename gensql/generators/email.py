import ctypes
from abc import ABC, abstractmethod
from typing import Callable, List

from .base import BaseGenerator
from .word import WordGenerator


class DomainGenerator(ABC):
    @abstractmethod
    def generate_chunk(self, chunk_size: int):
        pass


class StaticDomainGenerator(DomainGenerator):
    def __init__(self, domain: str):
        self.domain = domain

    def generate_chunk(self, chunk_size: int) -> bytearray:
        return bytearray(self.domain.encode("utf-8"))


class RandomDomainGenerator(DomainGenerator):
    def __init__(
        self,
        num_rows: int,
        domain_args: dict,
        shuffle_callback: Callable,
    ):
        self.domain_args = domain_args
        self.num_rows = num_rows
        self.shuffle_callback = shuffle_callback

    def generate_chunk(self, chunk_size: int):
        self.domain_args["byte_array"].reset_indices()
        domain_generator = WordGenerator(
            self.num_rows, **self.domain_args, shuffle_callback=self.shuffle_callback
        )

        yield domain_generator


class CommonRandomDomainGenerator(DomainGenerator):
    def __init__(
        self,
        num_rows: int,
        domain_args: dict,
        shuffle_callback: Callable,
    ):
        self.domain_args = domain_args
        self.num_rows = num_rows
        self.shuffle_callback = shuffle_callback

    def generate_chunk(self, chunk_size: int) -> bytearray:
        self.domain_args["byte_array"].reset_indices()
        domain_generator = WordGenerator(
            self.num_rows, **self.domain_args, shuffle_callback=self.shuffle_callback
        )
        domain = domain_generator.generate_chunk(chunk_size)
        return bytearray(next(domain)[0])


class BaseEmailGenerator(BaseGenerator):
    def __init__(
        self,
        num_rows: int,
        shuffle_callback: Callable,
        fname_args: dict,
        lname_args: dict,
    ):
        super().__init__(num_rows)

        fname_args["byte_array"].reset_indices()
        lname_args["byte_array"].reset_indices()

        self.fname_indices = fname_args["byte_array"].indices
        self.lname_indices = lname_args["byte_array"].indices

        self.fname_generator = WordGenerator(
            num_rows, **fname_args, shuffle_callback=shuffle_callback
        )
        self.lname_generator = WordGenerator(
            num_rows, **lname_args, shuffle_callback=shuffle_callback
        )

class EmailGenerator(BaseEmailGenerator):
    def __init__(
        self,
        num_rows: int,
        shuffle_callback: Callable,
        fname_args: dict,
        lname_args: dict,
        format_str: str,
        word: str,
        order: list,
        domain_args: dict,
    ):
        super().__init__(num_rows, shuffle_callback, fname_args, lname_args)
        self.domain_generator = WordGenerator(
            num_rows, **domain_args, shuffle_callback=shuffle_callback
        )

        self.format_str = format_str
        self.num_rows = num_rows
        self.order = order
        self.shuffle_callback = shuffle_callback

    def generate_chunk(self, chunk_size: int):
        generators = {
            "first_names": self.fname_generator,
            "last_names": self.lname_generator,
            "domains": self.domain_generator,
        }

        # this is ~20% faster than using a map for each chunk creation,
        # and still allows for ordering to be dictated
        zipped_chunks = zip(
            *(generators[name].generate_chunk(chunk_size) for name in self.order)
        )
        for x, y, z in zipped_chunks:
            emails = [
                self.format_str
                % (
                    x[i],
                    y[i],
                    z[i],
                )
                for i in range(chunk_size)
            ]
        yield emails
