from collections import defaultdict
from datetime import datetime, timedelta
import json
from math import floor
from os import urandom
import random
from sys import stderr
from textwrap import dedent

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
            filename = args.input or "skeleton.json"
            with open(filename, "r") as f:
                try:
                    schema = json.loads(f.read())
                except JSONDecodeError as e:
                    raise SystemExit(f"Error decoding schema\n{e}")
        except OSError as e:
            raise SystemExit(f"input schema {args.input} not found - generate one with the -g arg\n{e}")
        return schema

    def validate_schema(self, schema: dict):
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
            "json"
        ]
        pks = []
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
                raise SyntaxError(f"{k} is missing a type property")
            if not col_nullable:
                raise SyntaxError(f"{k} is missing a nullable property")
            if col_type not in allowed_cols:
                raise SyntaxError(f"{k}.{col_type} is not supported")
            if col_width and "char" not in col_type:
                raise SyntaxError(f"width is not a valid option for type {col_type}")
            if col_autoinc and "int" not in col_type:
                raise SyntaxError(f"auto increment is not a valid option for type {col_type}")
            if col_nullable and col_pk:
                raise SyntaxError(f"{k} is designated as a primary key and cannot be nullable")

            try:
                if col_type == "char" and 0 < int(col_width) < 2**8 :
                    raise SyntaxError(f"type {col_type} width must be in the range 0-{2**8 - 1}")
                if col_type == "varchar" and 0 < int(col_width) < 2**16:
                    raise SyntaxError(f"type {col_type} widh must be in the range 0-{2**16 - 1}")
            except ValueError:
                raise SyntaxError(f"{col_width} must be an integer")

        if len(pks) > 1:
            raise SyntaxError(f"cannot specify more than one primary key; got {[x for x in pks]}")

    def make_dates(self, num: int) -> list:
        """
        Makes n datetimes in a given range.
        strftime is actually extremely slow; this is faster.
        TODO: Fix invalid DST datetimes.
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
                #if not cols[col]["type"] in ("char", "varchar"):
                #    raise ValueError(
                #        f"width `{cols[col]['width']}` incorrectly specified for column `{col}` of type `{cols[col]['type']}`"
                    #)
                cols[col]["type"] = f"{cols[col]['type']} ({cols[col]['width']})"
            if cols.get(col, {}).get("nullable"):
                col_opts.append("NOT NULL")
            else:
                col_opts.append("NULL")
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
    def __init__(self, args, schema, tbl_cols, tbl_create):
        self.allocate = utilities.Allocator
        self.args = args
        self.schema = schema
        self.tbl_cols = tbl_cols
        self.tbl_create = tbl_create
        self.tbl_name = self.args.table
        self.monotonic_id = self.allocate(self.args.num)
        self.random_id = self.allocate(self.args.num, shuffle=True)
        self.unique_id = self.allocate(self.args.num, shuffle=True)
        try:
            with open("dates.txt", "r") as f:
                self.dates = f.readlines()
        except OSError:
            self.dates = Generator().make_dates(self.args.num)
        try:
            with open("first_names.txt", "r") as f:
                self.first_names = f.read().splitlines()
            with open("last_names.txt", "r") as f:
                self.last_names = f.read().splitlines()
        except OSError:
            print("FATAL: Unable to load names")
            raise SystemExit(1)
        self.num_rows_first_names = len(self.first_names)
        self.num_rows_last_names = len(self.last_names)

    def sample(self, iterable: list, num_rows: int) -> str:
        idx = floor(random.random() * num_rows)
        return iterable[idx]

    def make_row(self, schema: dict) -> list:
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
                    # Return an id for allocation 5% of the time
                    #if not i % 20:
                    #    self.random_id.release(row[col])

            elif "name" in col.lower():
                random_first = self.sample(self.first_names, self.num_rows_first_names)
                random_last = self.sample(self.last_names, self.num_rows_last_names)
                full_name = f"{random_last},{random_first}".replace("'", "''")
                row[col] = f"'{full_name}'"

            elif schema[col]["type"] == "timestamp":
                row[col] = date

        return row

    def make_sql_rows(self, vals: list) -> list:
        insert_rows = []
        insert_rows.append(f"LOCK TABLES `{self.tbl_name}` WRITE;\n")
        for row in vals:
            insert_rows.append(
                f"INSERT INTO `{self.tbl_name}` (`{'`, `'.join(self.tbl_cols)}`) VALUES ({row});\n"
            )
        insert_rows.append(f"UNLOCK TABLES;\n")

        return insert_rows

    def run(self):
        sql_inserts = []
        random.seed(urandom(4))
        for _ in range(1, self.args.num + 1):
            sql_inserts.append(self.make_row(self.schema))
        vals = [",".join(str(v) for v in d.values()) for d in sql_inserts]
        lines = self.make_sql_rows(vals)
        with open(args.output, "w") as f:
            f.writelines(self.tbl_create)
            f.writelines(lines)


if __name__ == "__main__":
    help = utilities.Help()
    args = utilities.Args().make_args()
    g = Generator()
    if args.extended_help:
        help.schema()
    elif args.generate:
        skeleton = help.make_skeleton()
        try:
            filename = args.output or "skeleton.json"
            with open(f"{filename}", f"{'w' if args.force else 'x'}") as f:
                f.write(skeleton)
            raise SystemExit(0)
        except FileExistsError as e:
            raise SystemExit(f"{filename} exists")
        except PermissionError as e:
            raise SystemExit(f"unable to write {filename}\n{e}")
    elif args.dates:
        try:
            filename = args.output or "dates.txt"
            with open(f"{filename}", f"{'w' if args.force else 'x'}") as f:
                dates = [x + "\n" for x in g.make_dates(args.num)]
                f.writelines(dates)
            raise SystemExit(0)
        except FileExistsError as e:
            raise SystemExit(f"--force not set, refusing to overwrite {filename}\n{e}")
        except PermissionError as e:
            raise SystemExit(f"unable to write {filename}\n{e}")
    tbl_name = args.table
    schema_dict = g.parse_schema()
    tbl_create, tbl_cols = g.mysql(schema_dict, tbl_name, args.drop_table)
    r = Runner(args, schema_dict, tbl_cols, tbl_create)
    r.run()
