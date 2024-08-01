from __future__ import annotations

import ctypes
import multiprocessing as mp
import os
import random
from typing import TYPE_CHECKING, Generator, List

from gensql.core.db import SQLiteColumnGetter
from gensql.core.worker import Worker
from gensql.generators.datetime import DateGenerator
from gensql.generators.email import DomainGenerator, EmailFmt, EmailGenerator
from gensql.generators.geo import CompositeGeoGenerator
from gensql.generators.uuid import UUIDGenerator, UUIDVersion
from gensql.generators.word import WordGenerator
# from gensql.utils.connections import SQLiteColumnGetter
from gensql.utils.shared_epoch import SharedEpochManager
from gensql.utils.shuffleable_bytes import ShuffleableBytes
from gensql.utils.writer import Writer

if TYPE_CHECKING:
    from gensql.core.worker import Worker

CHUNK_SIZE = 100_000
NUM_ROWS = 1_000_000
OUTPUT_FILE = "test.csv"


def load_shuffler() -> ctypes.CDLL:
    lib = ctypes.CDLL("./gensql/lib/bin/fast_shuffle.so")
    lib.shuf.argtypes = [
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_uint32,
        ctypes.c_uint32,
    ]
    return lib


def generate_chunk(
    generators: List[Worker], start_row: int, end_row: int, output_file: str
) -> None:
    chunk_size = end_row - start_row
    writer = Writer(f"{output_file}.part{start_row}")
    writer.start()

    for chunk in make_gen(generators, chunk_size):
        writer.write_chunk(chunk)

    writer.end()
    writer.join()


def setup_generators(num_rows: int) -> List[Worker]:
    fname_gen = WordGenerator(
        byte_list_fname, is_lower=False, chunk_size=CHUNK_SIZE, generator_name="fname"
    )
    lname_gen = WordGenerator(
        byte_list_lname, is_lower=False, chunk_size=CHUNK_SIZE, generator_name="lname"
    )
    domain_gen = DomainGenerator(byte_list_word, is_lower=True, chunk_size=CHUNK_SIZE)
    email_gen = EmailGenerator(
        fname_gen,
        lname_gen,
        domain_gen,
        email_fmt=EmailFmt(),
    )
    dt_gen = DateGenerator(
        min_dt="1995-05-23 00:00:00",
        max_dt="2038-01-01 00:00:00",
        shared_epoch_manager=shared_epoch_manager,
    )
    geo_gen = CompositeGeoGenerator(
        [byte_list_city, byte_list_state, byte_list_postcodes],
        chunk_size=CHUNK_SIZE,
        generator_name="geo",
    )
    uuid_gen = UUIDGenerator(
        uuid_version=UUIDVersion.VER_7, shared_epoch_manager=shared_epoch_manager
    )
    w_fname = Worker(
        fname_gen,
        NUM_ROWS,
        seeds={"fname": fname_seed},
        shuffle_lib=load_shuffler(),
    )
    w_lname = Worker(
        lname_gen,
        NUM_ROWS,
        seeds={"lname": lname_seed},
        shuffle_lib=load_shuffler(),
    )
    w_email = Worker(
        email_gen,
        NUM_ROWS,
        seeds={"fname": fname_seed, "lname": lname_seed, "domain": domain_seed},
        shuffle_lib=load_shuffler(),
    )
    w_dt = Worker(dt_gen, NUM_ROWS)
    w_uuid = Worker(uuid_gen, NUM_ROWS)

    w_geo = Worker(
        geo_gen,
        NUM_ROWS,
        seeds={"geo_0": geo_seed, "geo_1": geo_seed, "geo_2": geo_seed},
        shuffle_lib=load_shuffler(),
    )

    return [w_uuid, w_dt, w_fname, w_lname, w_email, w_geo]


def make_gen(funcs, num_rows: int) -> Generator:
    generators = [func.generate_column() for func in funcs]
    for chunks in zip(*generators):
        yield chunks


def generate_data(num_rows: int, output_file: str) -> None:
    generators = setup_generators(num_rows)
    writer = Writer(output_file)
    writer.start()

    for chunk in make_gen(generators, num_rows):
        writer.write_chunk(chunk)

    writer.end()
    writer.join()


if __name__ == "__main__":
    sql = SQLiteColumnGetter()

    fname_seed = random.getrandbits(32)
    lname_seed = random.getrandbits(32)
    domain_seed = random.getrandbits(32)
    geo_seed = random.getrandbits(32)

    # TODO: why am I getting max_lname etc.? Was it only for ShMem?
    data = sql.get_columns(["first_name"], "person_name")
    f_names = data["first_name"]
    data = sql.get_columns(["last_name"], "person_name")
    l_names = data["last_name"]
    data = sql.get_columns(["word"], "word")
    words = data["word"]
    data = sql.get_columns(
        ["postcode_us.city", "state_us.name", "postcode_us.code_post"],
        "postcode_us",
        "JOIN state_us ON postcode_us.code_state = state_us.code_state",
    )
    cities, states, postcodes = (
        data["postcode_us.city"],
        data["state_us.name"],
        data["postcode_us.code_post"],
    )
    byte_list_fname = ShuffleableBytes(f_names)
    byte_list_lname = ShuffleableBytes(l_names)
    byte_list_word = ShuffleableBytes(words)
    byte_list_city = ShuffleableBytes(cities)
    byte_list_state = ShuffleableBytes(states)
    byte_list_postcodes = ShuffleableBytes(postcodes)

    shared_epoch_manager = SharedEpochManager(name="datetimes", size=NUM_ROWS)

    generate_data(NUM_ROWS, OUTPUT_FILE)

    shared_epoch_manager.close_and_unlink()
