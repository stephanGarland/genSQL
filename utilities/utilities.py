import argparse
from collections import deque
import ctypes
import json
from os import urandom
import random
import sys
from textwrap import dedent


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
            self.lib = ctypes.CDLL("./library/fast_shuffle.so")
        except OSError as e:
            raise SystemExit(
                f"FATAL: couldn't load C library - run make\n\n{e}"
            ) from None
        self.lib.fill_array.argtypes = [ctypes.c_uint32]
        self.lib.fill_array.restype = ctypes.POINTER(ctypes.c_uint32)
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
            choices=["au", "de", "fr", "ke", "jp", "mx", "ua", "uk", "us"],
            default="us",
            help="The country's phone number structure to use if generating phone numbers",
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
            choices=["csv", "mysql", "postgresql", "sqlserver"],
            default="mysql",
            help="Filetype to generate",
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
            "--no-chunk",
            action="store_true",
            dest="no_chunk",
            help="Do not chunk SQL INSERT statements",
        )
        parser.add_argument(
            "-n", "--num", type=int, default=1000, help="The number of rows to generate"
        )
        parser.add_argument("-o", "--output", help="Output filename")
        parser.add_argument(
            "-r",
            "--random",
            action="store_true",
            help="Enable randomness on the length of some items",
        )
        parser.add_argument("-t", "--table", help="Table name to generate SQL for")
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
        return dedent(msg)

    def schema(self):
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
                * auto increment
            * json, text
                * max_length: float <0.01 - 1.00>
                  determines the maximum length of JSON arrays and TEXT columns
                  percentage - defaults to 0.15 which gives 4-wide JSON arrays
                  and 4 paragraphs of lorem ipsum text columns (~2900 chars)
            * all
                * default
                * invisible - NOTE: Only valid for MySQL
                * nullable - NOTE: absence implies true
                * primary key
                * unique
        e.g.
            {self.make_skeleton()}
        """
        print(dedent(msg))
        raise SystemExit(0)


class Utilities:
    def __init__(self):
        pass

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
        val = val.lower()
        if val in ("y", "yes", "t", "true", "on", "1"):
            return True
        elif val in ("n", "no", "f", "false", "off", "0"):
            return False
        else:
            raise ValueError(f"invalid truth value {val}")
