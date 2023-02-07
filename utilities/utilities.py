import argparse
from collections import deque
import sys
from textwrap import dedent

try:
    from numpy import arange, random
except ImportError:
    import random


class Allocator:
    def __init__(self, id_max: int, shuffle: bool = False):
        if "numpy" in sys.modules:
            self.id_list = arange(1, id_max + 1)
        else:
            self.id_list = [x for x in range(1, id_max + 1)]
        self.ids = deque(self.id_list)
        if shuffle:
            random.shuffle(self.ids)

    def allocate(self) -> int | None:
        try:
            return self.ids.popleft()
        except IndexError:
            return None

    def release(self, id: int):
        self.ids.appendleft(id)


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
            choices=["csv", "mysql", "postgres", "sqlserver", "txt"],
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
            "-n", "--num", type=int, default=1000, help="The number of rows to generate"
        )
        parser.add_argument("-o", "--output", help="Output filename")
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
                    "auto increment": "true",
                    "primary key": "true"
                },
                "name": {
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
                    "nullable": "true",
                    "default": "NULL"
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

        By default, the filename is used as the table name.

        Valid column types <sizes> are:
            * smallint [unsigned]
            * int [unsigned]
            * bigint [unsigned]
            * decimal
            * double
            * char: <0 - 2^8-1>
            * varchar: <0 - 2^16-1>
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
