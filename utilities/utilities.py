import argparse
from collections import deque
import sys

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
            "-d",
            "--generate-dates",
            action="store_true",
            dest="dates",
            help="Generate a file of datetimes for later use",
        )
        parser.add_argument(
            "-f",
            "--filetype",
            choices=["csv", "mysql", "postgres", "sqlserver", "txt"],
            help="Filetype to generate",
        )
        parser.add_argument(
            "-g",
            "--generate-skeleton",
            action="store_true",
            dest="generate",
            help="Generate a skeleton input JSON schema",
        )
        parser.add_argument(
            "-i", "--input", default="skeleton.json", help="Input schema (JSON) to generate data for"
        )
        parser.add_argument(
            "-n", "--num", type=int, default=1000, help="The number of rows to generate"
        )
        parser.add_argument("-o", "--output", default="test.sql", help="Output filename")
        parser.add_argument("-t", "--table", default="skeleton", help="Table name to generate SQL for")

        return parser.parse_args()

class Help:
    def __init__(self):
        pass

    def make_skeleton(self) -> str:
        return """
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

        The filename will be used as the table name.

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
                * length
            * integers
                * autoincrement
            * all
                * default
                * invisible - NOTE: Only valid for MySQL
                * nullable
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
        val = val.lower()
        if val in ("y", "yes", "t", "true", "on", "1"):
            return True
        elif val in ("n", "no", "f", "false", "off", "0"):
            return False
        else:
            raise ValueError(f"invalid truth value {val}")

