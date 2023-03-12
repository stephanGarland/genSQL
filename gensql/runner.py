import json
from math import ceil, floor
from os import urandom
import random
from pathlib import PurePath
from string import ascii_lowercase

from exceptions.exceptions import (
    OutputFilePermissionError,
    OverwriteFileError,
    TooManyRowsError,
)
from utilities.constants import (
    CITIES_COUNTRIES,
    COUNTRY_CODES,
    DEFAULT_INSERT_CHUNK_SIZE,
    DEFAULT_MAX_FIELD_PCT,
    JSON_OBJ_MAX_KEYS,
    JSON_OBJ_MAX_VALS,
    MYSQL_INT_MIN_MAX,
    PHONE_NUMBERS,
)
from gensql.generator import Generator
from utilities import utilities


class Runner:
    def __init__(self, args, schema, tbl_name, tbl_cols, tbl_create):
        self.allocator = utilities.Allocator
        self.args = args
        self.schema = schema
        self.tbl_cols = tbl_cols
        self.tbl_create = tbl_create
        self.tbl_name = tbl_name

        # TODO: speed this up
        if "country" in self.tbl_cols or "city" in self.tbl_cols:
            if self.args.country and not self.args.country == "random":
                self.cities = {
                    k: v
                    for k, v in CITIES_COUNTRIES.items()
                    if v == COUNTRY_CODES[self.args.country]
                }
            elif self.args.country == "random" and not "phone" in self.tbl_cols:
                self.cities = {k: v for k, v in CITIES_COUNTRIES.items()}
            elif self.args.country == "random" and "phone" in self.tbl_cols:
                valid_countries = {
                    v for k, v in COUNTRY_CODES.items() if k in PHONE_NUMBERS.keys()
                }
                self.cities = {
                    k: v
                    for k, v in CITIES_COUNTRIES.items()
                    if v in [x for x in COUNTRY_CODES.values() if x in valid_countries]
                }
            self.num_rows_cities = len(self.cities)

        _has_monotonic = False
        _has_unique = False

        # exceeding auto_increment capacity is checked at schema validation, but since
        # the user can specify --validate without passing --num, uniques have to be checked here
        numeric_cols = {
            k: v
            for k, v in self.schema.items()
            if "int" in v["type"] or v["type"] in ["decimal", "double", "int"]
        }
        for k, v in numeric_cols.items():
            if "unsigned" in v["type"]:
                col_max_val = MYSQL_INT_MIN_MAX[
                    f"MYSQL_MAX_{v['type'].upper().split()[0]}_UNSIGNED"
                ]
            elif v["type"] in ["decimal", "double"]:
                col_max_val = float("inf")
            else:
                col_max_val = MYSQL_INT_MIN_MAX[
                    f"MYSQL_MAX_{v['type'].upper().split()[0]}_SIGNED"
                ]
            if v.get("auto_inc"):
                _has_monotonic = True
            if v.get("unique"):
                _has_unique = True
                self.rand_max_id = self.args.num
                if self.args.num > col_max_val:
                    raise TooManyRowsError(k, self.args.num, col_max_val) from None
            # if uniquity isn't required, and the requested number of rows is greater
            # than the column can handle, just set it to the column's max since we can repeat
            else:
                if self.args.num > col_max_val:
                    self.rand_max_id = col_max_val
                else:
                    self.rand_max_id = self.args.num

        if _has_monotonic:
            self.monotonic_id = self.allocator(0, self.args.num)
        try:
            self.random_id = self.allocator(0, self.rand_max_id, shuffle=True)
        except AttributeError:
            self.random_id = self.allocator(0, self.args.num, shuffle=True)
        if _has_unique:
            self.unique_id = self.allocator(0, self.args.num, shuffle=True)
        try:
            with open("content/dates.txt", "r") as f:
                self.dates = f.readlines()
        except FileNotFoundError:
            self.dates = Generator(self.args).make_dates(self.args.num)
        try:
            if "first_name" in self.tbl_cols or "full_name" in self.tbl_cols:
                with open("content/first_names.txt", "r") as f:
                    self.first_names = f.read().splitlines()
                self.num_rows_first_names = len(self.first_names)
            if "last_name" in self.tbl_cols or "full_name" in self.tbl_cols:
                with open("content/last_names.txt", "r") as f:
                    self.last_names = f.read().splitlines()
                self.num_rows_last_names = len(self.last_names)
            if "email" in self.tbl_cols or [
                "json" in x.values() for x in self.tbl_cols.values()
            ]:
                with open("content/wordlist.txt", "r") as f:
                    self.wordlist = f.read().splitlines()
                self.num_rows_wordlist = len(self.wordlist)
            if ["text" in x.values() for x in self.tbl_cols.values()]:
                with open("content/lorem_ipsum.txt", "r") as f:
                    self.lorem_ipsum = f.read().splitlines()
                self.num_rows_lorem_ipsum = len(self.lorem_ipsum)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"unable to load necessary content\n{e}")

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

    def make_row(self, idx: int, has_timestamp: bool) -> dict:
        row = {}
        if has_timestamp:
            date = self.sample(self.dates, self.args.num)
        for col, opts in self.schema.items():
            if opts.get("is_empty"):
                continue
            if "int" in opts.get("type"):
                if opts.get("auto_increment"):
                    # may or may not just remove this, for now pass is fine
                    pass
                    # row[col] = self.monotonic_id.allocate()
                elif opts.get("unique"):
                    row[col] = self.unique_id.allocate()
                else:
                    row[col] = self.random_id.allocate()
                    # these are appended to the right of the deque, so they won't be immediately repeated
                    self.random_id.release(row[col])

            elif opts.get("type") in ["decimal", "double"]:
                # TODO: Figure out why this fails with unique checks
                if opts.get("unique"):
                    whole = self.unique_id.allocate()
                    fractional = str(idx)[0]
                    row[col] = f"'{whole}.{fractional}{whole}'"
                    self.unique_id.release(whole)
                else:
                    whole = self.random_id.allocate()
                    fractional = str(idx)[0]
                    row[col] = f"'{whole}.{fractional}{whole}'"
                    self.random_id.release(whole)

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

            elif opts.get("type") == "json":
                max_rows_pct = float(opts.get("max_length", DEFAULT_MAX_FIELD_PCT))
                if self.args.random:
                    json_arr_len = ceil(
                        random.random() * (JSON_OBJ_MAX_VALS - 1) * max_rows_pct
                    )
                else:
                    json_arr_len = ceil((JSON_OBJ_MAX_VALS - 1) * max_rows_pct)
                if self.schema[col].get("is_numeric_array"):
                    rand_id_list = []
                    # make 5% of the JSON arrays filled with random integers
                    if not idx % 20:
                        for i in range(json_arr_len):
                            rand_id = self.random_id.allocate()
                            rand_id_list.append(str(rand_id))
                            self.random_id.release(rand_id)
                        rand_ids = ",".join(rand_id_list)
                        row[col] = f"'[{rand_ids}]'"
                    else:
                        row[col] = "'[]'"
                else:
                    json_dict = {}
                    # may or may not want to use this again
                    # json_keys = self.sample(
                    #    self.wordlist, self.num_rows_wordlist, JSON_OBJ_MAX_KEYS
                    # )
                    json_keys = [f"{x}_key" for x in ascii_lowercase]
                    # grab an extra for use with email if needed
                    json_vals = self.sample(
                        self.wordlist, self.num_rows_wordlist, JSON_OBJ_MAX_VALS + 1
                    )
                    json_dict[json_keys.pop(0)] = json_vals.pop()
                    # make 20% of the JSON objects nested
                    if not self.args.fixed_length:
                        if not idx % 5:
                            key = json_keys.pop(0)
                            json_dict[key] = {}
                            json_dict[key][json_keys.pop(0)] = [
                                json_vals.pop() for _ in range(json_arr_len)
                            ]
                    else:
                        key = json_keys.pop(0)
                        json_dict[key] = {}
                        json_dict[key][json_keys.pop(0)] = [
                            json_vals.pop() for _ in range(json_arr_len)
                        ]
                    row[col] = f"'{json.dumps(json_dict)}'"

            elif col == "city":
                if self.args.country and not self.args.country == "random":
                    city = self.sample(
                        list(self.cities.keys()), self.num_rows_cities
                    ).replace("'", "''")
                else:
                    city = self.sample(
                        list(self.cities.keys()), self.num_rows_cities
                    ).replace("'", "''")
                row[col] = f"'{city}'"
            elif col == "country":
                if self.args.country and not self.args.country == "random":
                    country = list(self.cities.values())[0]
                else:
                    try:
                        country = self.cities[city]
                    except (KeyError, UnboundLocalError):
                        country = self.sample(
                            list(self.cities.values()), self.num_rows_cities
                        ).replace("'", "''")
                row[col] = f"'{country}'"

            elif col == "email":
                try:
                    email_domain = json_vals.pop()
                except UnboundLocalError:
                    email_domain = self.sample(self.wordlist, self.num_rows_wordlist)
                try:
                    email_local = f"{random_first}.{random_last}"
                except UnboundLocalError:
                    email_first = self.sample(
                        self.first_names, self.num_rows_first_names
                    )
                    email_last = self.sample(self.last_names, self.num_rows_last_names)
                    email_local = f"{email_first}.{email_last}"
                email_local = email_local.lower().replace("'", "''")
                row[col] = f"'{email_local}@{email_domain}.com'"
            elif col == "phone":
                phone_digits = [str(x) for x in range(10)]
                random.shuffle(phone_digits)
                phone_str = "".join(phone_digits)
                row[col] = f"'{PHONE_NUMBERS[self.args.country](phone_str)}'"
            elif self.schema[col]["type"] == "text":
                max_rows_pct = float(opts.get("max_length", DEFAULT_MAX_FIELD_PCT))
                # e.g. if max_rows_pct is 0.15, with 25 rows in lorem ipsum, we get a range of 1-4 rows
                if not self.args.fixed_length:
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

            elif opts.get("type") == "timestamp":
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
        insert_rows.append("SET @@autocommit=0;\n")
        insert_rows.append("SET @@unique_checks=0;\n")
        insert_rows.append(f"LOCK TABLES `{self.tbl_name}` WRITE;\n")
        if not self.args.no_chunk:
            for i in range(0, len(vals), DEFAULT_INSERT_CHUNK_SIZE):
                insert_rows.append(
                    f"INSERT INTO `{self.tbl_name}` (`{'`, `'.join(self.tbl_cols)}`) VALUES\n"
                )
                chunk_list = vals[i : i + DEFAULT_INSERT_CHUNK_SIZE]
                for row in chunk_list:
                    insert_rows.append(f"({row}),\n")
                # if we reach the end of a chunk list, make the multi-insert statement a single
                # query by swapping the last comma to a semi-colon
                insert_rows[-1] = insert_rows[-1][::-1].replace(",", ";", 1)[::-1]
        else:
            for row in vals:
                insert_rows.append(
                    f"INSERT INTO `{self.tbl_name}` (`{'`, `'.join(self.tbl_cols)}`) VALUES ({row});\n"
                )
        insert_rows.append("COMMIT;\n")
        insert_rows.append("SET @@autocommit=1;\n")
        insert_rows.append("SET @@unique_checks=1;\n")
        insert_rows.append("SET @@time_zone=(SELECT @@global.time_zone);\n")
        insert_rows.append(f"UNLOCK TABLES;\n")

        return insert_rows

    def run(self) -> str:
        sql_inserts = []
        random.seed(urandom(4))
        _has_timestamp = any("timestamp" in s.values() for s in self.schema.values())
        unique_cols = [k for k, v in self.schema.items() if v.get("unique")]
        seen_rows = set()
        for i in range(1, self.args.num + 1):
            row = self.make_row(i, _has_timestamp)
            if not self.args.no_check:
                for unique in unique_cols:
                    if row[unique] in seen_rows:
                        # TODO: expand this beyond only emails
                        counter = 1
                        email_split = row[unique].split("@")
                        new_email = f"{email_split[0]}_{counter}@{email_split[1]}"
                        # don't spend forever trying to de-duplicate
                        while new_email in seen_rows:
                            if counter > 9:
                                self.logger.warning(f"unable to de-duplicate {row[unique]}")
                                break
                            counter += 1
                            new_email = f"{email_split[0]}_{counter}@{email_split[1]}"
                        row[unique] = new_email
                        seen_rows.add(row[unique])
                    else:
                        seen_rows.add(row[unique])
            sql_inserts.append(row)
        vals = [",".join(str(v) for v in d.values()) for d in sql_inserts]
        match self.args.filetype:
            case "mysql":
                lines = self.make_sql_rows(vals)
                try:
                    filename = f"{PurePath(self.args.output).with_suffix('.sql')}"
                except TypeError:
                    try:
                        filename = f"{PurePath(self.args.input).stem}.sql"
                    except TypeError:
                        filename = "gensql.sql"
            case "csv":
                lines = self.make_csv_rows(vals)
                try:
                    filename = f"{PurePath(self.args.output).with_suffix('.csv')}"
                except TypeError:
                    try:
                        filename = f"{PurePath(self.args.input).stem}.csv"
                    except TypeError:
                        filename = "gensql.csv"
            case _:
                raise ValueError(f"{self.args.filetype} is not a valid output format")
        try:
            with open(
                f"schema_outputs/{filename}", f"{'w' if self.args.force else 'x'}"
            ) as f:
                if "sql" in self.args.filetype:
                    f.writelines(self.tbl_create)
                if self.args.filetype == "csv":
                    with open(
                        f"schema_outputs/tbl_{self.tbl_name}_create.sql",
                        f"{'w' if self.args.force else 'x'}",
                    ) as ft:
                        ft.writelines(self.tbl_create)
                f.writelines(lines)
                if not self.args.quiet and self.args.filetype == "csv":
                    print("SET @@time_zone = '+00:00';")
                    if not self.args.no_check:
                        print("SET @@unique_checks=0;")
                    print(
                        f"LOAD DATA INFILE '{filename}' INTO TABLE `{self.tbl_name}` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY \"'\" IGNORE 1 LINES (`{'`, `'.join(self.tbl_cols)}`);"
                    )
        except FileExistsError:
            raise OverwriteFileError(filename) from None
        except PermissionError:
            raise OutputFilePermissionError(filename) from None
        return f"schema_outputs/{filename}"
