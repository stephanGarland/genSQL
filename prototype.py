import ctypes
import random
import time

from gensql.generators.datetime import DateGenerator
from gensql.generators.email import DomainGenerator, EmailFmt, EmailGenerator
from gensql.generators.uuid import UUIDGenerator, UUIDVersion
from gensql.generators.word import WordGenerator
from gensql.utils.connections import SQLiteColumnGetter
from gensql.utils.shuffleable_byte_array import ShuffleableByteArray
from gensql.utils.writer import Writer
from gensql.core.worker import Worker

CHUNK_SIZE = 100_000
NUM_ROWS = 1_000_000


def load_shuffler():
    lib = ctypes.CDLL("./gensql/lib/bin/fast_shuffle.so")
    lib.shuf.argtypes = [
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_uint32,
        ctypes.c_uint32,
    ]
    return lib


def make_gen(funcs):
    generators = [func.generate_column() for func in funcs]
    for chunks in zip(*generators):
        yield chunks


if __name__ == "__main__":
    sql = SQLiteColumnGetter

    fname_seed = random.getrandbits(32)
    lname_seed = random.getrandbits(32)
    domain_seed = random.getrandbits(32)

    max_fname, f_names = sql("first_name", "person_name").get_col()
    max_lname, l_names = sql("last_name", "person_name").get_col()
    max_word, words = sql("word", "word").get_col()

    byte_array_fname = ShuffleableByteArray(f_names)
    byte_array_lname = ShuffleableByteArray(l_names)
    byte_array_word = ShuffleableByteArray(words)

    fname_gen = WordGenerator(
        byte_array_fname, is_lower=False, chunk_size=CHUNK_SIZE, generator_name="fname"
    )
    lname_gen = WordGenerator(
        byte_array_lname, is_lower=False, chunk_size=CHUNK_SIZE, generator_name="lname"
    )
    domain_gen = DomainGenerator(byte_array_word, is_lower=True, chunk_size=CHUNK_SIZE)
    email_gen = EmailGenerator(
        fname_gen,
        lname_gen,
        domain_gen,
        email_fmt=EmailFmt(),
    )
    dt_gen = DateGenerator(min_dt="1995-05-23 00:00:00", max_dt="2038-01-01 00:00:00")
    uuid_gen = UUIDGenerator(uuid_version=UUIDVersion.VER_7)
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

    writer = Writer("test.csv")
    writer.start()

    for chunk in make_gen([w_uuid, w_fname, w_lname, w_email, w_dt]):
        writer.write_chunk(chunk)

    writer.end()
    writer.join()
