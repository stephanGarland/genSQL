import argparse
import ctypes
import json
import random
import sqlite3
from collections import deque
from math import floor
from os import urandom
from textwrap import dedent


# TODO: Implement a textual allocator by multiplying the wordlist by N, then creating a monotonic allocator below matching its length, and shuffling it as needed
class Allocator:
    def __init__(
        self, id_min: int, id_max: int, ranged_arr: bool = False, shuffle: bool = False
    ):
        random.seed(urandom(8))
        self.c_rand_seed = random.getrandbits(32)
        self.id_min = id_min
        self.id_max = id_max
        self.id_range = self.id_max - self.id_min
        try:
            self.lib = ctypes.CDLL("./gensql/utils/libs/fast_shuffle.so")
        except OSError as e:
            raise SystemExit(
                f"FATAL: couldn't load C library - run `run.sh` or `make`\n\n{e}"
            ) from None
        self.lib.fill_array.argtypes = [ctypes.c_uint32]
        self.lib.fill_array.restype = ctypes.POINTER(ctypes.c_uint32)
        self.lib.fill_array_range.argtypes = [ctypes.c_uint32, ctypes.c_uint32]
        self.lib.fill_array_range.restype = ctypes.POINTER(ctypes.c_uint32)
        self.lib.shuf.argtypes = [
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.c_uint32,
            ctypes.c_uint32,
        ]
        if not ranged_arr:
            self.id_list_ptr = self.lib.fill_array(self.id_max)
        else:
            self.id_list_ptr = self.lib.fill_array_range(self.id_min, self.id_max)
        if shuffle:
            self.lib.shuf(self.id_list_ptr, self.id_range, self.c_rand_seed)
        self.id_list = (ctypes.c_uint32 * self.id_range).from_address(
            ctypes.addressof(self.id_list_ptr.contents)
        )
        self.ids = deque(self.id_list)

    def allocate(self) -> int | None:
        try:
            return self.ids.popleft()
        except IndexError:
            return None

    def release(self, id: int):
        """
        IDs are appended to the right so that it creates
        an infinite loop of IDs, as this is only used for non-uniques.
        TODO, maybe: re-shuffle once a loop has completed.
        """
        self.ids.append(id)


class PhoneAllocator:
    def __init__(self, phone_min: int, phone_max: int):
        # phone_max + 1 so the range is inclusive
        self.allocator = Allocator(
            phone_min, phone_max + 1, ranged_arr=True, shuffle=True
        )
        try:
            self.lib = ctypes.CDLL("./gensql/utils/libs/fast_mod.so")
        except OSError as e:
            raise SystemExit(
                f"FATAL: couldn't load C library - run `run.sh` or `make`\n\n{e}"
            ) from None

        self.lib.fastmod_u32.argtypes = [
            ctypes.c_uint32,
            ctypes.c_uint64,
            ctypes.c_uint32,
        ]
        self.lib.precompute_M_u32.argtypes = [ctypes.c_uint32]
        self.lib.is_divisible.argtypes = [ctypes.c_uint32, ctypes.c_uint64]
        self.lib.fastmod_u32.restype = ctypes.c_uint32
        self.lib.is_divisible.restype = ctypes.c_bool
        self.lib.precompute_M_u32.restype = ctypes.c_uint64
        self.M_value = self.lib.precompute_M_u32(11)

    def allocate(self) -> int | None:
        try:
            candidate = self.allocator.ids.popleft()
            while self.lib.is_divisible(candidate, self.M_value):
                candidate = self.allocator.ids.popleft()
            return candidate
        except IndexError:
            return None


class UUIDAllocator:
    def __init__(self, num: int, use_uuid_v4: bool = True):
        try:
            self.lib = ctypes.CDLL("./gensql/utils/libs/uuid.so")
        except OSError as e:
            raise SystemExit(
                f"FATAL: couldn't load C library - run `run.sh` or `make`\n\n{e}"
            ) from None
        self.lib.fill_array.argtypes = [ctypes.c_int, ctypes.c_bool]
        self.lib.fill_array.restype = ctypes.POINTER(ctypes.c_char_p)
        self.num = num
        if use_uuid_v4:
            self.uuid_ptr = self.lib.fill_array(self.num, True)
        else:
            self.uuid_ptr = self.lib.fill_array(self.num, False)
        self.uuid_list = [self.uuid_ptr[i].decode() for i in range(self.num)]
        self.uuids = deque(self.uuid_list)

    def allocate(self) -> str | None:
        try:
            return self.uuids.popleft()
        except IndexError:
            return None


class Args:
    def __init__(self):
        pass

    def make_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--extended-help",
            action="store_true",
            dest="extended_help",
            help="Print extended help",
        )
        parser.add_argument(
            "--country",
            choices=["random", "au", "de", "fr", "gb", "ke", "jp", "mx", "ua", "us"],
            default="random",
            help="A specific country (or random) to use for cities, phone numbers, etc.",
        )
        parser.add_argument(
            "-d", "--debug", action="store_true", help="Print tracebacks for errors"
        )
        parser.add_argument(
            "--drop-table",
            action="store_true",
            dest="drop_table",
            help="WARNING: DESTRUCTIVE - use DROP TABLE with generation",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="WARNING: DESTRUCTIVE - overwrite any files",
        )
        parser.add_argument(
            "-f",
            "--filetype",
            choices=["csv", "mysql", "postgres"],
            default="mysql",
            help="Filetype to generate",
        )
        parser.add_argument(
            "--fixed-length",
            action="store_true",
            dest="fixed_length",
            help="Disable any variations in length for JSON arrays, text, etc.",
        )
        parser.add_argument(
            "--generate-dates",
            action="store_true",
            dest="dates",
            help="Generate a file of datetimes for later use",
        )
        parser.add_argument(
            "-g",
            "--generate-skeleton",
            action="store_true",
            dest="generate",
            help="Generate a skeleton input JSON schema",
        )
        parser.add_argument("-i", "--input", help="Input schema (JSON)")
        parser.add_argument(
            "--no-check",
            action="store_true",
            dest="no_check",
            help="Do not perform validation checks for unique columns",
        )
        parser.add_argument(
            "--no-chunk",
            action="store_true",
            dest="no_chunk",
            help="Do not chunk SQL INSERT statements",
        )
        parser.add_argument(
            "-n",
            "--num",
            type=int,
            default=1000,
            help="The number of rows to generate - defaults to 1000",
        )
        parser.add_argument(
            "-o", "--output", help="Output filename - defaults to gensql"
        )
        parser.add_argument(
            "-q",
            "--quiet",
            action="store_true",
            help="Suppress printing various informational messages",
        )
        parser.add_argument(
            "-r",
            "--random",
            action="store_true",
            help="Enable randomness on the length of some items",
        )
        parser.add_argument(
            "-t",
            "--table",
            help="Table name to generate SQL for - defaults to the filename",
        )
        parser.add_argument("--validate", help="Validate an input JSON schema")
        return parser.parse_args()


class Help:
    def __init__(self):
        pass

    def make_skeleton(self) -> str:
        msg = """
            {
                "user_id": {
                    "type": "bigint unsigned",
                    "nullable": "false",
                    "auto_increment": "true",
                    "primary_key": "true"
                },
                "full_name": {
                    "type": "varchar",
                    "width": "255",
                    "nullable": "false"
                },
                "external_id": {
                    "type": "bigint unsigned",
                    "nullable": "false",
                    "unique": "true",
                    "default": "0"
                },
                "last_modified": {
                    "type": "timestamp",
                    "nullable": "false",
                    "default": "now()"
                }
            }
        """
        return msg

    def extended_help(self):
        msg = f"""
        GenSQL expects a JSON input schema, of the format:

            {{
                "col_name": {{
                    "col_type": "type",
                    "col_option_0": "option",
                    "col_option_n": "option"
                }}
            }}

        Valid column types <sizes> are:
            * smallint [unsigned]
            * int [unsigned]
            * bigint [unsigned]
            * decimal
            * double
            * char: <1 - 2^8-1>
            * varchar: <1 - 2^16-1>
            * timestamp
            * text
            * json

            NOTE: for char and varchar, you must also specify a size.
            NOTE: unsigned is only valid for MySQL.

        Valid column options are:
            * [var]char
                * width
            * integers
                * auto_increment
            * json, text
                * max_length: float <0.01 - 1.00>
                  determines the maximum length of JSON arrays and TEXT columns
                  percentage - defaults to 0.15 which gives 4-wide JSON arrays
                  and 4 paragraphs of lorem ipsum text columns (~2900 chars)
            * all
                * default
                * invisible - NOTE: Only valid for MySQL
                * is_id
                    * provides hints to gensql on whether or not to create integers
                      for a column if it cannot be automatically inferred
                * nullable - NOTE: absence implies true
                * primary_key
                * unique
        Valid default values are:
            * any constant
            * array()
                * creates a json array
            * now()
                * for a timestamp column, creates a default null value, and
                  automatically updates with the current time if the row is updated
            * static_now()
                * for a timestamp column, creates a default value of the current time
                  when the schema is loaded into a database
        e.g.
            {self.make_skeleton()}
        """
        print(dedent(msg))
        raise SystemExit(0)


class Utilities:
    def sample(
        self, iterable: list, num_rows: int, num_samples: int = 1
    ) -> list[str] | str:
        sample_list = []
        for i in range(num_samples):
            idx = floor(random.random() * num_rows)
            if num_samples == 1:
                return iterable[idx]
            sample_list.append(iterable[idx])
        return sample_list

    def lowercase_schema(self, schema: dict) -> dict:
        """
        Allows input schemas to be correctly parsed if uppercase
        letters are used (e.g. NULL as a default) without doing repeated
        lower() calls during row creation.
        """
        if isinstance(schema, dict):
            return {k.lower(): self.lowercase_schema(v) for k, v in schema.items()}
        elif isinstance(schema, list):
            return [self.lowercase_schema(v) for v in schema]
        elif isinstance(schema, str):
            return schema.lower()
        else:
            return schema

    # Farewell, distutils
    def strtobool(self, val) -> bool:
        """Convert a string representation of truth to true (1) or false (0).
        True values are "y", "yes", "t", "true", "on", and "1"; false values
        are "n", "no", "f", "false", "off", and "0".  Raises ValueError if
        "val" is anything else.
        """
        if val is None:
            return False
        try:
            val = val.lower()
        except AttributeError:
            val = str(val).lower()
        if val in ("y", "yes", "t", "true", "on", "1"):
            return True
        elif val in ("n", "no", "f", "false", "off", "0"):
            return False
        else:
            raise ValueError(f"invalid truth value {val}")