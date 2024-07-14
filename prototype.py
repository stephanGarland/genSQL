import time
import random

from gensql.generators.email import EmailGenerator
from gensql.generators.uuid import UUIDGenerator
from gensql.generators.word import WordGenerator
from gensql.utils.byte_array import ByteArray
from gensql.utils.connections import SQLiteColumnGetter
from gensql.utils.writer import Writer
from gensql.worker import Worker

CHUNK_SIZE = 100_000
NUM_ROWS = 1_000_000


def make_gen(funcs):
    generators = [func.generate_column() for func in funcs]
    for chunks in zip(*generators):
        yield chunks


# TODO: have this do ordering as well
def make_ordered_format(formatting: dict) -> bytes:
    """
    :formatting
        {
            "separator": "." | "",
            "sections": [
                {
                    "name": str,
                    "length": int  # -1 indicates unlimited
                }
            ]
        }
    """
    format_str = []
    for i, key in enumerate(formatting["sections"]):
        if key["name"] != "domain":
            format_str.append("%")
        if key["length"] > 0:
            format_str.append(f".{key['length']}")
        if key["name"] != "domain":
            format_str.append("b")
        if i == 0:
            format_str.append(formatting["separator"])
        if key["name"] == "domain":
            format_str.append("@%b.com")
    return "".join(format_str).encode("utf-8")


if __name__ == "__main__":
    sql = SQLiteColumnGetter

    seed = random.getrandbits(32)

    max_fname, f_names = sql("first_name", "person_name").get_col()
    max_lname, l_names = sql("last_name", "person_name").get_col()
    max_word, words = sql("word", "word").get_col()

    byte_array_fname = ByteArray(f_names)
    byte_array_lname = ByteArray(l_names)
    byte_array_word = ByteArray(words)

    email_fmt_str = {
        "separator": ".",
        "sections": [
            {"name": "first", "length": -1},
            {"name": "last", "length": -1},
            {"name": "domain", "length": -1},
        ],
    }

    w_fname = Worker(
        NUM_ROWS,
        byte_array_fname.seeds,
        WordGenerator,
        {
            "word": "first_name",
            "byte_array": byte_array_fname,
        },
        seed,
    )
    w_lname = Worker(
        NUM_ROWS,
        byte_array_lname.seeds,
        WordGenerator,
        {
            "word": "last_name",
            "byte_array": byte_array_lname,
        },
        seed,
    )
    w_email = Worker(
        NUM_ROWS,
        byte_array_word.seeds,
        EmailGenerator,
        {
            "word": "email",
            "fname_args": {
                "is_lower": True,
                "word": "first_name",
                "byte_array": byte_array_fname,
            },
            "lname_args": {
                "is_lower": True,
                "word": "last_name",
                "byte_array": byte_array_lname,
            },
            "format_str": make_ordered_format(email_fmt_str),
            "order": ["first_names", "last_names", "domains"],
            "domain_args": {
                "word": "domain",
                "byte_array": byte_array_word,
            },
        },
        seed,
    )

    w_uuid = Worker(
        NUM_ROWS,
        {},
        UUIDGenerator,
        {"uuid_v4": True, "uuid_v7": False, "word": "uuid"},
        seed,
    )

    writer = Writer("test.csv")
    writer.start()

    for chunk in make_gen([w_fname, w_lname, w_email, w_uuid]):
        writer.write_chunk(chunk)

    writer.end()
    writer.join()
