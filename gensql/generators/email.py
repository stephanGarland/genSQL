import ctypes
from typing import Callable

from .base import BaseGenerator
from .base_combined import CombinedGenerator
from .word import WordGenerator

# TODO: have formatting options, e.g.
# stephan.garland@common.tld
# s.garland@common.tld
# sgarland@common.tld
# garland.stephan@common.tld
# first.last@random.tld


class EmailGenerator(BaseGenerator):
    def __init__(
        self,
        num_rows: int,
        word: str,
        shuffle_callback: Callable,
        fname_args: dict,
        lname_args: dict,
        domain_args: dict,
    ):
        super().__init__(num_rows)
        fname_args["byte_array"].reset_indices()
        lname_args["byte_array"].reset_indices()
        domain_args["byte_array"].reset_indices()
        self.fname_indices = fname_args["byte_array"].indices
        self.lname_indices = lname_args["byte_array"].indices
        self.domain_indices = domain_args["byte_array"].indices
        self.first_name_generator = WordGenerator(
            num_rows, **fname_args, shuffle_callback=shuffle_callback
        )
        self.last_name_generator = WordGenerator(
            num_rows, **lname_args, shuffle_callback=shuffle_callback
        )
        self.domain_generator = WordGenerator(
            num_rows, **domain_args, shuffle_callback=shuffle_callback
        )
        self.shuffle_callback = shuffle_callback

    def generate_chunk(self, chunk_size: int):
        first_name_chunks = self.first_name_generator.generate_chunk(chunk_size)
        last_name_chunks = self.last_name_generator.generate_chunk(chunk_size)
        domain_chunks = self.domain_generator.generate_chunk(chunk_size)

        for first_names, last_names, domains in zip(
            first_name_chunks, last_name_chunks, domain_chunks
        ):
            emails: list = []

            for i in range(chunk_size):
                email = bytearray()
                email += first_names[i].lower()
                email += b"."
                email += last_names[i].lower()
                email += b"@"
                email += domains[i]
                email += b".com"
                emails.append(email)
            yield emails
