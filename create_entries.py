from collections import defaultdict
from datetime import datetime, timedelta
import json
from math import ceil, floor
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
    TooManyRowsError,
)

from utilities.constants import (
    DEFAULT_INSERT_CHUNK_SIZE,
    DEFAULT_MAX_FIELD_PCT,
    DEFAULT_VARYING_LENGTH,
    JSON_OBJ_MAX_KEYS,
    JSON_OBJ_MAX_VALS,
    MYSQL_INT_MIN_MAX,
)
from utilities import utilities


class Generator:
    def __init__(self, args):
        self.args = args
        self.utils = utilities.Utilities()
        self.start_date = datetime(1995, 5, 23)
        self.end_date = datetime.now()

    def parse_schema(self) -> dict[str, dict[str, str]]:
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

        def _add_error(error_schema: dict, key: tuple, value: dict, error_message: str):
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
            col_max_length = v.get("max_length")
            col_pk = self.utils.strtobool(v.get("primary key"))
            col_unique = self.utils.strtobool(v.get("unique"))
            if "int" in col_type:
                if "unsigned" in col_type:
                    col_min_val = 0
                else:
                    col_min_val = MYSQL_INT_MIN_MAX[
                        f"MYSQL_MIN_{col_type.upper().split()[0]}_SIGNED"
                    ]
                col_max_val = MYSQL_INT_MIN_MAX[
                    f"MYSQL_MAX_{col_type.upper().split()[0]}_UNSIGNED"
                ]
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
            if col_autoinc and args.num > col_max_val:
                _add_error(
                    errors,
                    (k, "type"),
                    v,
                    f"column type `{col_type}` cannot hold the maximum value specified ({self.args.num})",
                )
            if col_max_length and col_type not in ["json", "text"]:
                _add_error(
                    errors,
                    (k, "max_length"),
                    v,
                    f"max_length is not a valid option for column `{k}` of type `{col_type}`",
                )
            if col_nullable and col_pk:
                _add_error(
                    errors,
                    (k, "nullable"),
                    v,
                    f"column `{k}` is designated as a primary key and cannot be nullable",
                )
            if "text" in col_type and col_default:
                _add_error(
                    errors,
                    (k, "default"),
                    v,
                    f"default is not a valid option for column `{k}` of type `{col_type}`",
                )
            try:
                if col_type == "char" and not 0 < int(col_width) < 2**8:
                    _add_error(
                        errors,
                        (k, "width"),
                        v,
                        f"column `{k}` of type `{col_type}` width must be in the range 1-{2**8 - 1} (got {col_width})",
                    )
                if col_type == "varchar" and not 0 < int(col_width) < 2**16:
                    _add_error(
                        errors,
                        (k, "width"),
                        v,
                        f"column `{k}` of type `{col_type}` width must be in the range 1-{2**16 - 1} (got {col_width})",
                    )
            except ValueError:
                _add_error(
                    errors,
                    (k, "width"),
                    v,
                    f"{col_width} must be an integer",
                )
            try:
                if col_max_length and not 0 < float(col_max_length) <= 1:
                    _add_error(
                        errors,
                        (k, "max_length"),
                        v,
                        f"column `{k}` of type `{col_type}` max_length must be in the range 0.01-1.00 (got {col_max_length})",
                    )
            except ValueError:
                _add_error(
                    errors,
                    (k, "max_length"),
                    v,
                    f"{col_max_length} must be a float",
                )
        if len(pks) > 1:
            _add_error(
                errors,
                (k, "primary key"),
                v,
                f"cannot specify more than one primary key; got {[x for x in pks]}",
            )
        error_len = len([x.keys() for x in errors["schema"].values() if "error" in x])
        if error_len:
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
        self, schema: dict[str, dict[str, str]], tbl_name: str, drop_table: bool = False
    ) -> tuple[str, dict[str, str]]:
        auto_inc_exists = False
        msg = ""
        pk = None
        recursive_dict = lambda: defaultdict(recursive_dict)  # type: ignore
        cols = recursive_dict()
        col_defs = {}
        uniques = []

        if drop_table:
            msg += f"DROP TABLE IF EXISTS `{tbl_name}`;\n"
        msg += f"CREATE TABLE `{tbl_name}` (\n"
        for col, col_attributes in schema.items():
            col_opts = []
            for k, v in col_attributes.items():
                match k:
                    case "type":
                        cols[col]["type"] = v
                    case "auto increment":
                        cols[col]["auto_inc"] = self.utils.strtobool(v)
                    case "default":
                        cols[col]["default"] = v
                    case "invisible":
                        cols[col]["invisible"] = self.utils.strtobool(v)
                    case "width":
                        cols[col]["width"] = v
                    case "max_length":
                        pass
                    case "nullable":
                        cols[col]["nullable"] = self.utils.strtobool(v)
                    case "primary key":
                        cols[col]["pk"] = True
                        pk = col
                    case "unique":
                        cols[col]["unique"] = True
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
        self.allocator = utilities.Allocator
        self.args = args
        self.schema = schema
        self.tbl_cols = tbl_cols
        self.tbl_create = tbl_create
        self.tbl_name = tbl_name

        # exceeding auto_increment capacity is checked at schema validation, but since
        # the user can specify --validate without passing --num, uniques have to be checked here
        id_cols = {k: v for k, v in self.tbl_cols.items() if "id" in k}
        for k, v in id_cols.items():
            if "unsigned" in v["type"]:
                col_max_val = MYSQL_INT_MIN_MAX[
                    f"MYSQL_MAX_{v['type'].upper().split()[0]}_UNSIGNED"
                ]
            else:
                col_max_val = MYSQL_INT_MIN_MAX[
                    f"MYSQL_MAX_{v['type'].upper().split()[0]}_SIGNED"
                ]

            if v.get("unique"):
                if self.args.num > col_max_val:
                    raise TooManyRowsError(k, self.args.num, col_max_val) from None
            else:
                if self.args.num > col_max_val:
                    self.rand_max_id = col_max_val
                else:
                    self.rand_max_id = self.args.num

        self.monotonic_id = self.allocator(self.args.num)
        self.random_id = self.allocator(self.rand_max_id, shuffle=True)
        self.unique_id = self.allocator(self.args.num, shuffle=True)
        try:
            with open("content/dates.txt", "r") as f:
                self.dates = f.readlines()
        except FileNotFoundError:
            self.dates = Generator(self.args).make_dates(self.args.num)
        try:
            with open("content/first_names.txt", "r") as f:
                self.first_names = f.read().splitlines()
            with open("content/last_names.txt", "r") as f:
                self.last_names = f.read().splitlines()
            with open("content/wordlist.txt", "r") as f:
                self.wordlist = f.read().splitlines()
            with open("content/lorem_ipsum.txt", "r") as f:
                self.lorem_ipsum = f.read().splitlines()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"unable to load necessary content\n{e}")
        self.num_rows_first_names = len(self.first_names)
        self.num_rows_last_names = len(self.last_names)
        self.num_rows_lorem_ipsum = len(self.lorem_ipsum)
        self.num_rows_wordlist = len(self.wordlist)

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

    def make_row(self, schema: dict, idx: int) -> dict:
        row = {}
        if any("timestamp" in s.values() for s in schema.values()):
            date = self.sample(self.dates, self.args.num)
        for col, opts in schema.items():
            if "id" in col:
                if schema.get(col, {}).get("auto increment"):
                    row[col] = self.monotonic_id.allocate()
                elif schema.get(col, {}).get("unique"):
                    row[col] = self.unique_id.allocate()
                else:
                    row[col] = self.random_id.allocate()

                    # these are appended to the right of the deque, so they won't be immediately repeated
                    self.random_id.release(row[col])

            elif col == "first_name":
                random_first = self.sample(self.first_names, self.num_rows_first_names)
                first_name = f"{random_first}".replace("'", "''")
                row[col] = f"'{first_name}'"

            elif col == "last_name":
                random_last = self.sample(self.last_names, self.num_rows_last_names)
                last_name = f"{random_last}".replace("'", "''")
                row[col] = f"'{last_name}'"

            elif col == "full_name":
                random_first = self.sample(self.first_names, self.num_rows_first_names)
                random_last = self.sample(self.last_names, self.num_rows_last_names)
                full_name = f"{random_last}, {random_first}".replace("'", "''")
                row[col] = f"'{full_name}'"

            elif schema[col]["type"] == "json":
                json_dict = {}
                keys = self.sample(
                    self.wordlist, self.num_rows_wordlist, JSON_OBJ_MAX_KEYS
                )
                vals = self.sample(
                    self.wordlist, self.num_rows_wordlist, JSON_OBJ_MAX_VALS
                )
                json_dict[keys.pop()] = vals.pop()
                max_rows_pct = float(
                    schema.get(col, {}).get("max_length", DEFAULT_MAX_FIELD_PCT)
                )
                if self.args.random:
                    json_arr_len = ceil(
                        random.random() * (JSON_OBJ_MAX_VALS - 1) * max_rows_pct
                    )
                else:
                    json_arr_len = ceil((JSON_OBJ_MAX_VALS - 1) * max_rows_pct)
                # make 20% of the JSON objects nested with a list object of length
                if not idx % 5:
                    key = keys.pop()
                    json_dict[key] = {}
                    json_dict[key][keys.pop()] = [
                        vals.pop() for _ in range(json_arr_len)
                    ]
                row[col] = f"'{json.dumps(json_dict)}'"

            elif schema[col]["type"] == "text":
                max_rows_pct = float(
                    schema.get(col, {}).get("max_length", DEFAULT_MAX_FIELD_PCT)
                )
                # e.g. if max_rows_pct is 0.15, with 25 rows in lorem ipsum, we get a range of 1-4 rows
                if DEFAULT_VARYING_LENGTH:
                    if self.args.random:
                        lorem_rows = ceil(
                            random.random() * self.num_rows_lorem_ipsum * max_rows_pct
                        )
                    # otherwise, default to a single row, but 20% of the time use the maximum allowed
                    else:
                        lorem_rows = 1
                        if not idx % 5:
                            lorem_rows = ceil(self.num_rows_lorem_ipsum * max_rows_pct)
                else:
                    lorem_rows = 1
                if lorem_rows > 1:
                    row[
                        col
                    ] = f"'{' '.join(self.sample(self.lorem_ipsum, self.num_rows_lorem_ipsum, lorem_rows))}'"
                # sample() returns a string rather than a list if n=1, so skip that entirely and just use the first row of lorem
                else:
                    row[col] = f"'{self.lorem_ipsum[0]}'"

            elif schema[col]["type"] == "timestamp":
                row[col] = date
        return row

    def make_csv_rows(self, vals: list) -> list:
        insert_rows = []
        insert_rows.append(f"{','.join(self.tbl_cols)}\n")
        for row in vals:
            insert_rows.append(f"{row}\n")

        return insert_rows

    def make_sql_rows(self, vals: list) -> list:
        insert_rows = []
        insert_rows.append("SET @@time_zone = '+00:00';\n")
        insert_rows.append("SET autocommit=0;\n")
        insert_rows.append("SET unique_checks=0;\n")
        insert_rows.append(f"LOCK TABLES `{self.tbl_name}` WRITE;\n")
        if self.args.chunk:
            for i in range(0, len(vals), DEFAULT_INSERT_CHUNK_SIZE):
                insert_rows.append(
                    f"INSERT INTO `{self.tbl_name}` (`{'`, `'.join(self.tbl_cols)}`) VALUES\n"
                )
                chunk_list = vals[i : i + DEFAULT_INSERT_CHUNK_SIZE]
                for row in chunk_list:
                    insert_rows.append(f"({row}),\n")
                # if we reach the end of a chunk list, make the multi-insert a commit by swapping
                # the last comma to a semi-colon
                insert_rows[-1] = insert_rows[-1][::-1].replace(",", ";", 1)[::-1]
        else:
            for row in vals:
                insert_rows.append(
                    f"INSERT INTO `{self.tbl_name}` (`{'`, `'.join(self.tbl_cols)}`) VALUES ({row});\n"
                )
        insert_rows.append("COMMIT;\n")
        insert_rows.append("SET autocommit=1;\n")
        insert_rows.append("SET unique_checks=1;\n")
        insert_rows.append(f"UNLOCK TABLES;\n")

        return insert_rows

    def run(self):
        sql_inserts = []
        random.seed(os.urandom(4))
        for i in range(1, self.args.num + 1):
            row = self.make_row(self.schema, i)
            sql_inserts.append(row)
        vals = [",".join(str(v) for v in d.values()) for d in sql_inserts]
        match args.filetype:
            case "mysql":
                lines = self.make_sql_rows(vals)
                filename = args.output or "test.sql"
            case "csv":
                lines = self.make_csv_rows(vals)
                filename = args.output or "test.csv"
            case _:
                raise ValueError(f"{args.filetype} is not a valid output format")
        try:
            with open(filename, f"{'w' if args.force else 'x'}") as f:
                if "sql" in args.filetype:
                    f.writelines(self.tbl_create)
                if args.filetype == "csv":
                    with open("tbl_create.sql", f"{'w' if args.force else 'x'}") as ft:
                        ft.writelines(self.tbl_create)
                f.writelines(lines)
        except FileExistsError:
            raise OverwriteFileError(filename) from None
        except PermissionError:
            raise OutputFilePermissionError(filename) from None


if __name__ == "__main__":
    utils = utilities.Utilities()
    h = utilities.Help()
    args = utilities.Args().make_args()
    g = Generator(args)
    if not args.debug:
        sys.tracebacklimit = 0
    if args.extended_help:
        h.schema()
    elif args.generate:
        skeleton = h.make_skeleton()
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
    if args.chunk and "sql" not in args.filetype:
        print(f"WARNING: --chunk has no effect on {args.filetype} output, ignoring")
    tbl_name = args.table or "gensql"
    schema_dict = g.parse_schema()
    schema_dict = utils.lowercase_schema(schema_dict)
    g.validate_schema(schema_dict)
    tbl_create, tbl_cols = g.mysql(schema_dict, tbl_name, args.drop_table)
    r = Runner(args, schema_dict, tbl_name, tbl_cols, tbl_create)
    r.run()
