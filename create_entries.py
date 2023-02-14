from collections import defaultdict
from datetime import datetime, timedelta
import json
from math import floor
import os
from pprint import pprint
import random
from stat import filemode
import sys
from textwrap import dedent

from exceptions.exceptions import (
    OutputFilePermissionError,
    OverwriteFileError,
    SchemaValidationError,
)
from utilities import utilities


class Generator:
    def __init__(self):
        self.utils = utilities.Utilities()
        self.start_date = datetime(1995, 5, 23)
        self.end_date = datetime.now()

    def parse_schema(self) -> dict[str, str]:
        """
        Parses input schema in JSON format and returns
        a dictionary of the required columns and their types.

        Does not perform any validation that the requested
        column names or types are valid.
        """

        try:
            filename = args.input or args.validate or "skeleton.json"
            with open(filename, "r") as f:
                try:
                    schema = json.loads(f.read())
                except json.JSONDecodeError as e:
                    raise SystemExit(f"Error decoding schema\n{e}")
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"input schema {args.input} not found - generate one with the -g arg\n{e}"
            )
        return schema

    def validate_schema(self, schema: dict) -> bool:
        """
        Validates that a JSON schema can be parsed for use by GenSQL.
        If the schema is invalid, the error and its location in the file will
        be added to the error_schema dict with _add_error(), to then be raised
        to SchemaValidationError at the end.
        """

        def _add_error(
            error_schema: dict, key: tuple, value: dict, error_message: str
        ):
            """
            Adds errors to the error_schema dict. Expects a tuple of two elements
            as the key, of the format (`column_name`, `column_option_key`), as well
            as the specific error message to be appended to the invalid schema.
            """
            try:
                error_schema[key] = v
                error_schema[key]["error"] = error_message
            except KeyError:
                raise

        allowed_cols = [
            "smallint",
            "smallint unsigned",
            "int",
            "int unsigned",
            "bigint",
            "bigint unsigned",
            "decimal",
            "double",
            "char",
            "varchar",
            "timestamp",
            "text",
            "json",
        ]
        pks = []
        errors = {}
        errors["schema"] = schema
        for k, v in schema.items():
            col_type = v.get("type")
            col_width = v.get("width")
            col_nullable = self.utils.strtobool(v.get("nullable"))
            col_autoinc = self.utils.strtobool(v.get("auto increment"))
            col_default = v.get("default")
            col_invisible = self.utils.strtobool(v.get("invisible"))
            col_pk = self.utils.strtobool(v.get("primary key"))
            col_unique = self.utils.strtobool(v.get("unique"))
            if col_pk:
                pks.append(k)
            if not col_type:
                _add_error(
                    errors,
                    (k, "type"),
                    v,
                    f"column `{k}` is missing a type property",
                )
            if col_type not in allowed_cols:
                _add_error(
                    errors,
                    (k, "type"),
                    v,
                    f"column type `{col_type}` is not supported",
                )
            if col_width and "char" not in col_type:
                _add_error(
                    errors,
                    (k, "width"),
                    v,
                    f"column type `{col_type}` is not supported",
                )
            if col_autoinc and "int" not in col_type:
                _add_error(
                    errors,
                    (k, "auto increment"),
                    v,
                    f"auto increment is not a valid option for column `{k}` of type `{col_type}`",
                )
            if col_nullable and col_pk:
                _add_error(
                    errors,
                    (k, "nullable"),
                    v,
                    f"column `{k}` is designated as a primary key and cannot be nullable",
                )
            try:
                if col_type == "char" and not 0 < int(col_width) < 2**8:
                    _add_error(
                        errors,
                        (k, "width"),
                        v,
                        f"column `{k}` of type `{col_type}` width must be in the range 0-{2**8 - 1} (got {col_width})",
                    )
                if col_type == "varchar" and not 0 < int(col_width) < 2**16:
                    _add_error(
                        errors,
                        (k, "width"),
                        v,
                        f"column `{k}` of type `{col_type}` width must be in the range 0-{2**16 - 1} (got {col_width})",
                    )
            except ValueError:
                _add_error(
                    errors,
                    (k, "width"),
                    v,
                    f"{col_width} must be an integer",
                )
        if len(pks) > 1:
            _add_error(
                errors,
                (k, "primary key"),
                v,
                f"cannot specify more than one primary key; got {[x for x in pks]}",
            )
        if len(errors) > len(schema):
            raise SchemaValidationError(
                errors, "found errors validating schema - see above"
            )
        else:
            return True

    def make_dates(self, num: int) -> list:
        """
        Makes n datetimes in a given range.
        strftime is actually extremely slow; this is faster.
        """
        dates = []
        delta = self.end_date - self.start_date
        delta_int = delta.days * 86400 + delta.seconds
        for _ in range(num):
            rand_int = floor(random.random() * delta_int)
            new_date = self.start_date + timedelta(seconds=rand_int)
            yr = f"{new_date.year:04d}"
            mo = f"{new_date.month:02d}"
            da = f"{new_date.day:02d}"
            hr = f"{new_date.hour:02d}"
            mi = f"{new_date.minute:02d}"
            se = f"{new_date.second:02d}"
            new_datestr = f"'{yr}-{mo}-{da} {hr}:{mi}:{se}'"
            dates.append(new_datestr)
        return dates

    def mysql(
        self, schema: dict[str, str], tbl_name: str, drop_table: bool = False
    ) -> tuple[str, dict[str, str]]:
        auto_inc_exists = False
        msg = ""
        pk = None
        recursive_dict = lambda: defaultdict(recursive_dict)
        cols = recursive_dict()
        col_defs = {}
        uniques = []

        if drop_table:
            msg += f"DROP TABLE IF EXISTS `{tbl_name}`;\n"
        msg += f"CREATE TABLE `{tbl_name}` (\n"
        for col, col_attributes in schema.items():
            col_opts = []
            for k, v in col_attributes.items():
                match (k.lower()):
                    case "type":
                        cols[col]["type"] = v
                    case "auto increment":
                        cols[col]["auto_inc"] = self.utils.strtobool(v)
                    case "default":
                        if not "nul" in v.lower():
                            cols[col]["default"] = v
                    case "invisible":
                        cols[col]["invisible"] = self.utils.strtobool(v)
                    case "width":
                        cols[col]["width"] = v
                    case "nullable":
                        cols[col]["nullable"] = self.utils.strtobool(v)
                    case "primary key":
                        cols[col]["pk"] = True
                        pk = col
                    case "unique":
                        uniques.append(col)
                    case _:
                        raise ValueError(f"column attribute {k} is invalid")
            if cols[col]["width"]:
                cols[col]["type"] = f"{cols[col]['type']} ({cols[col]['width']})"
            if cols.get(col, {}).get("nullable"):
                col_opts.append("NULL")
            else:
                col_opts.append("NOT NULL")
            if cols.get(col, {}).get("default"):
                col_opts.append(f"DEFAULT {cols[col]['default'].upper()}")
            if cols.get(col, {}).get("invisible"):
                col_opts.append("INVISIBLE")
            if cols.get(col, {}).get("auto_inc"):
                col_opts.append("AUTO_INCREMENT")
                auto_inc_exists = True

            col_defs[
                col
            ] = f"  `{col}` {cols[col]['type']}{' ' + ' '.join(col_opts) if col_opts else ''},"
        msg += "\n".join(col_defs.values())
        msg += f"\n  PRIMARY KEY (`{pk}`),\n"
        for i, u in enumerate(uniques, 1):
            msg += f"  UNIQUE KEY {u} (`{u}`)"
            if not i == len(uniques) and len(uniques) > 1:
                msg += ",\n"
            else:
                msg += "\n"
        msg += f") ENGINE=InnoDB {'AUTO_INCREMENT=0' if auto_inc_exists else ''} DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
        return (msg, cols)


class Runner:
    def __init__(self, args, schema, tbl_name, tbl_cols, tbl_create):
        self.allocate = utilities.Allocator
        self.args = args
        self.schema = schema
        self.tbl_cols = tbl_cols
        self.tbl_create = tbl_create
        self.tbl_name = tbl_name
        self.monotonic_id = self.allocate(self.args.num)
        self.random_id = self.allocate(self.args.num, shuffle=True)
        self.unique_id = self.allocate(self.args.num, shuffle=True)
        try:
            with open("content/dates.txt", "r") as f:
                self.dates = f.readlines()
        except FileNotFoundError:
            self.dates = Generator().make_dates(self.args.num)
        try:
            with open("content/first_names.txt", "r") as f:
                self.first_names = f.read().splitlines()
            with open("content/last_names.txt", "r") as f:
                self.last_names = f.read().splitlines()
            with open("content/wordlist.txt", "r") as f:
                self.wordlist = f.read().splitlines()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"unable to load necessary content\n{e}")
        self.num_rows_first_names = len(self.first_names)
        self.num_rows_last_names = len(self.last_names)
        self.num_rows_wordlist = len(self.wordlist)

    def sample(self, iterable: list, num_rows: int) -> str:
        idx = floor(random.random() * num_rows)
        return iterable[idx]

    def make_row(self, schema: dict, idx: int) -> list:
        row = {}
        if any("timestamp" in s.values() for s in schema.values()):
            date = self.sample(self.dates, self.args.num)
        for col, opts in schema.items():
            if "id" in col.lower():
                if schema.get(col, {}).get("auto increment"):
                    row[col] = self.monotonic_id.allocate()
                elif schema.get(col, {}).get("unique"):
                    row[col] = self.unique_id.allocate()
                else:
                    row[col] = self.random_id.allocate()
                    # Return an id for allocation 2% of the time
                    if idx % 100 < 2:
                        self.random_id.release(row[col])

            elif col.lower() == "first_name":
                random_first = self.sample(self.first_names, self.num_rows_first_names)
                row[col] = f"{random_first}".replace("'", "''")
            elif col.lower() == "last_name":
                random_last = self.sample(self.last_names, self.num_rows_last_names)
                row[col] = f"{random_last}".replace("'", "''")
            elif col.lower() == "full_name":
                random_first = self.sample(self.first_names, self.num_rows_first_names)
                random_last = self.sample(self.last_names, self.num_rows_last_names)
                full_name = f"{random_last}, {random_first}".replace("'", "''")
                row[col] = f"'{full_name}'"

            elif schema[col]["type"] == "json":
                json_dict = {}
                # create an object of maximum depth 4
                last_key = self.sample(self.wordlist, self.num_rows_wordlist)
                for x in range(idx % 5):
                    # this needs work; it just sets a new key to the last val
                    random_val = self.sample(self.wordlist, self.num_rows_wordlist)
                    last_key = json_dict.setdefault(last_key, random_val)

            elif schema[col]["type"] == "timestamp":
                row[col] = date

        return row

    def make_sql_rows(self, vals: list) -> list:
        insert_rows = []
        insert_rows.append("SET @@time_zone = '+00:00';\n")
        insert_rows.append(f"LOCK TABLES `{self.tbl_name}` WRITE;\n")
        for row in vals:
            insert_rows.append(
                f"INSERT INTO `{self.tbl_name}` (`{'`, `'.join(self.tbl_cols)}`) VALUES ({row});\n"
            )
        insert_rows.append(f"UNLOCK TABLES;\n")

        return insert_rows

    def run(self):
        sql_inserts = []
        random.seed(os.urandom(4))
        for i in range(1, self.args.num + 1):
            sql_inserts.append(self.make_row(self.schema, i))
        vals = [",".join(str(v) for v in d.values()) for d in sql_inserts]
        lines = self.make_sql_rows(vals)
        filename = args.output or "test.sql"
        try:
            with open(filename, f"{'w' if args.force else 'x'}") as f:
                f.writelines(self.tbl_create)
                f.writelines(lines)
        except FileExistsError:
            raise OverwriteFileError(filename) from None
        except PermissionError:
            raise OutputFilePermissionError(filename) from None


if __name__ == "__main__":
    help = utilities.Help()
    args = utilities.Args().make_args()
    g = Generator()
    if not args.debug:
        sys.tracebacklimit = 0
    if args.extended_help:
        help.schema()
    elif args.generate:
        skeleton = help.make_skeleton()
        try:
            filename = args.output or "skeleton.json"
            with open(f"{filename}", f"{'w' if args.force else 'x'}") as f:
                f.write(skeleton)
            raise SystemExit(0)
        except FileExistsError:
            raise OverwriteFileError(filename) from None
        except PermissionError:
            raise OutputFilePermissionError(filename) from None
    elif args.validate:
        if g.validate_schema(g.parse_schema()):
            print("INFO: validated schema, no errors detected")
            raise SystemExit(0)
    elif args.dates:
        try:
            filename = args.output or "content/dates.txt"
            with open(f"{filename}", f"{'w' if args.force else 'x'}") as f:
                dates = [x + "\n" for x in g.make_dates(args.num)]
                f.writelines(dates)
            raise SystemExit(0)
        except FileExistsError:
            raise OverwriteFileError(filename) from None
        except PermissionError:
            raise OutputFilePermissionError(filename) from None
    tbl_name = args.table or "gensql"
    schema_dict = g.parse_schema()
    tbl_create, tbl_cols = g.mysql(schema_dict, tbl_name, args.drop_table)
    r = Runner(args, schema_dict, tbl_name, tbl_cols, tbl_create)
    r.run()
