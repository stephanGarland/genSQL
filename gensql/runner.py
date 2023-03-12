from copy import copy
from math import ceil, floor
from os import urandom
import random
from pathlib import PurePath
import sqlite3

from exceptions.exceptions import (
    OutputFilePermissionError,
    OverwriteFileError,
    TooManyRowsError,
)
from gensql.generator import Generator
from utilities.constants import (
    DEFAULT_INSERT_CHUNK_SIZE,
    DEFAULT_MAX_FIELD_PCT,
    JSON_DEFAULT_KEYS,
    JSON_OBJ_MAX_KEYS,
    JSON_OBJ_MAX_VALS,
    MYSQL_INT_MIN_MAX,
    PHONE_NUMBERS,
)
from utilities import logger, utilities


class Runner:
    def __init__(self, args, schema, tbl_name, tbl_cols, tbl_create, unique_cols):
        self.allocator = utilities.Allocator
        self.args = args
        self.city_country_swapped = False
        self.logger = logger.Logger().logger
        self.schema = schema
        self.tbl_cols = tbl_cols
        self.tbl_create = tbl_create
        self.tbl_name = tbl_name
        self.utils = utilities.Utilities()
        self.unique_cols = unique_cols
        conn = sqlite3.connect("db/gensql.db")
        cursor = conn.cursor()
        if "country" in self.tbl_cols or "city" in self.tbl_cols:
            if "country" in self.tbl_cols and "city" in self.tbl_cols:
                self.city_index = [
                    i for i, (k, v) in enumerate(self.schema.items()) if k == "city"
                ][0]
                self.country_index = [
                    i for i, (k, v) in enumerate(self.schema.items()) if k == "country"
                ][0]
                # city must be evaluated first for proper country selection
                # but we can swap them back at the end to match the desired schema
                if self.city_index > self.country_index:
                    self.logger.info(
                        "Performing in-memory city/country swap to match schema"
                    )
                    self.city_country_swapped = True
                    temp_schema = list(self.schema.items())
                    temp_schema[self.city_index], temp_schema[self.country_index] = (
                        temp_schema[self.country_index],
                        temp_schema[self.city_index],
                    )
                    self.schema = dict(temp_schema)
            if self.args.country and not self.args.country == "random":
                city_query = f"SELECT c.city, c.country FROM cities c WHERE c.country = (SELECT cc.country FROM countries cc WHERE cc.code = '{self.args.country.upper()}')"
                cursor.execute(city_query)
                self.cities, self.countries = zip(*cursor.fetchall())
            elif self.args.country == "random" and not "phone" in self.tbl_cols:
                city_query = "SELECT c.city, c.country FROM cities c"
                cursor.execute(city_query)
                self.cities, self.countries = zip(*cursor.fetchall())
            elif self.args.country == "random" and "phone" in self.tbl_cols:
                city_query = f"SELECT c.city, c.country FROM cities c WHERE c.country IN ((SELECT cc.country FROM countries cc WHERE cc.code IN ({PHONE_NUMBERS.keys()}))"
                cursor.execute(city_query)
                self.cities, self.countries = zip(*cursor.fetchall())
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
        conn.close()

    # TODO: try making multiple smaller random id lists to be able to skip random.random() calls
    def sample(
        self, iterable: list, num_rows: int, num_samples: int = 1
    ) -> list[str] | str:
        sample_list = []
        for i in range(num_samples):
            idx = floor(random.random() * num_rows)
            # idx = self.random_id.allocate()
            if num_samples == 1:
                # self.random_id.release(idx)
                return iterable[idx]
            sample_list.append(iterable[idx])
            # self.random_id.release(idx)
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
                    # this is a horror show, but it _is_ slightly (4%) faster the built-in json.dumps()
                    # when 3.12 lands with its dumps() speedup, this can probably go away
                    json_obj = []
                    json_keys = copy(JSON_DEFAULT_KEYS)
                    json_vals = self.sample(
                        self.wordlist, self.num_rows_wordlist, JSON_OBJ_MAX_VALS + 1
                    )
                    json_obj.append(f'"{json_keys.pop()}"')
                    json_obj.append(f'"{json_vals.pop()}",')
                    # make 20% of the JSON objects nested
                    if not self.args.fixed_length:
                        if not idx % 5:
                            json_obj.append(f'"{json_keys.pop()}"')
                            json_obj.append(f'"{json_keys.pop()}"')
                            for _ in range(json_arr_len):
                                json_obj.append(f'"{json_vals.pop()}",')
                    else:
                        json_obj.append(f'"{json_keys.pop()}"')
                        json_obj.append(f'"{json_keys.pop()}"')
                        for _ in range(json_arr_len):
                            json_obj.append(f'"{json_vals.pop()}",')
                    col_json = "'{"
                    stop_iter = False
                    json_obj_len = len(json_obj)
                    for idx, item in enumerate(json_obj):
                        if idx + 1 == json_obj_len:
                            stop_iter = True
                        if item[2] == "_":
                            if json_obj[idx + 1][2] == "_":
                                col_json += f"{item}: {{"
                            else:
                                col_json += f"{item}: "
                        else:
                            if (
                                not stop_iter
                                and not json_obj[idx + 1][2] == "_"
                                and "[" not in col_json
                            ):
                                col_json += f"[{item}"
                            else:
                                col_json += f"{item}"
                            if stop_iter:
                                if "[" in col_json:
                                    col_json += "]"
                    for _ in range(max(1, 25 - len(json_keys))):
                        col_json += "}"
                    col_json += "'"
                    col_json = col_json[::-1].replace(",", "", 1)[::-1]
                    row[col] = col_json
            elif col == "city":
                city = self.sample(list(self.cities), self.num_rows_cities).replace(
                    "'", "''"
                )
                row[col] = f"'{city}'"
            elif col == "country":
                if self.args.country and not self.args.country == "random":
                    country = self.countries[0]
                else:
                    try:
                        country = self.utils.get_country(city)
                    except (KeyError, UnboundLocalError):
                        # since city is guaranteed to come first, if this is hit
                        # there is no city column defined in the schema
                        country = self.sample(
                            list(self.countries), self.num_rows_cities
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
        seen_rows = set()
        for i in range(1, self.args.num + 1):
            row = self.make_row(i, _has_timestamp)
            if not self.args.no_check:
                for unique in self.unique_cols:
                    if row[unique] in seen_rows:
                        # TODO: expand this beyond only emails
                        counter = 1
                        email_split = row[unique].split("@")
                        new_email = f"{email_split[0]}_{counter}@{email_split[1]}"
                        # don't spend forever trying to de-duplicate
                        while new_email in seen_rows:
                            if counter > 9:
                                self.logger.warning(
                                    f"unable to de-duplicate {row[unique]}"
                                )
                                break
                            counter += 1
                            new_email = f"{email_split[0]}_{counter}@{email_split[1]}"
                        row[unique] = new_email
                        seen_rows.add(row[unique])
                    else:
                        seen_rows.add(row[unique])
            sql_inserts.append(row)
        if self.city_country_swapped:
            for insert in sql_inserts:
                insert["city"], insert["country"] = insert["country"], insert["city"]
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
