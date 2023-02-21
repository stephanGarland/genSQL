from collections import defaultdict
from datetime import datetime, timedelta
import json
from math import floor
from pathlib import PurePath
import random

from exceptions.exceptions import SchemaValidationError
from utilities.constants import MYSQL_INT_MIN_MAX

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
            if self.args.input:
                filename = PurePath(self.args.input).stem
            elif self.args.validate:
                filename = PurePath(self.args.validate).stem
            else:
                filename = "skeleton"
            with open(f"{filename}.json", "r") as f:
                try:
                    schema = json.loads(f.read())
                except json.JSONDecodeError as e:
                    raise SystemExit(f"Error decoding schema\n{e}")
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"input schema {self.args.input} not found - generate one with the -g arg\n{e}"
            ) from None
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
            "bigint unsigned",
            "bigint",
            "char",
            "decimal",
            "double",
            "email",
            "int unsigned",
            "int",
            "json",
            "phone",
            "smallint unsigned",
            "smallint",
            "text",
            "timestamp",
            "varchar",
        ]
        pks = []
        errors = {}
        errors["schema"] = schema
        for k, v in schema.items():
            col_type = v.get("type")
            col_width = v.get("width")
            col_nullable = self.utils.strtobool(v.get("nullable"))
            col_autoinc = self.utils.strtobool(v.get("auto_increment"))
            col_default = v.get("default")
            col_invisible = self.utils.strtobool(v.get("invisible"))
            col_max_length = v.get("max_length")
            col_pk = self.utils.strtobool(v.get("primary_key"))
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
            if k == "phone" and "char" not in col_type:
                _add_error(
                    errors,
                    (k, "type"),
                    v,
                    f"column `{k}` must be of type CHAR or VARCHAR",
                )
            if k == "phone" and col_unique:
                _add_error(
                    errors,
                    (k, "unique"),
                    v,
                    f"unique is not a valid option for column `{k}` - this is a performance decision; numbers are still unlikely to collide",
                )
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
            if col_default == "array()" and not col_type == "json":
                _add_error(
                    errors,
                    (k, "default"),
                    v,
                    f"array is not a valid default value for column `{k}` of type `{col_type}`",
                )
            if col_default in ["now()", "static_now()"] and not col_type == "timestamp":
                _add_error(
                    errors,
                    (k, "default"),
                    v,
                    f"{col_default} is not a valid default value for column `{k}` of type `{col_type}`",
                )
            if col_width and "char" not in col_type:
                _add_error(
                    errors,
                    (k, "width"),
                    v,
                    f"width is not a valid option for column `{k}` of type `{col_type}`",
                )
            if col_autoinc and "int" not in col_type:
                _add_error(
                    errors,
                    (k, "auto_increment"),
                    v,
                    f"auto_increment is not a valid option for column `{k}` of type `{col_type}`",
                )
            if col_autoinc and self.args.num > col_max_val:
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
                (k, "primary_key"),
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
                    case "auto_increment":
                        cols[col]["auto_inc"] = self.utils.strtobool(v)
                    case "default":
                        cols[col]["default"] = v
                    case "invisible":
                        cols[col]["invisible"] = self.utils.strtobool(v)
                    case "width":
                        cols[col]["width"] = v
                    case "is_id":
                        pass
                    case "max_length":
                        pass
                    case "nullable":
                        cols[col]["nullable"] = self.utils.strtobool(v)
                    case "primary_key":
                        cols[col]["pk"] = True
                        pk = col
                    case "unique":
                        cols[col]["unique"] = True
                        uniques.append(col)
                    case _:
                        raise ValueError(f"column attribute {k} is invalid")
            if cols[col]["width"]:
                cols[col]["type"] = f"{cols[col]['type']} ({cols[col]['width']})"
            if self.utils.strtobool(col_attributes.get("nullable", "true")):
                col_opts.append("NULL")
            else:
                col_opts.append("NOT NULL")
            col_default = col_attributes.get("default")
            # this is here as an early exit, not for any kind of type checking
            if col_default:
                if col_default == "null":
                    col_opts.append("DEFAULT NULL")
                elif col_default == "array()":
                    col_opts.append("DEFAULT (JSON_ARRAY())")
                elif col_default.isdigit():
                    col_opts.append(f"DEFAULT {col_default}")
                elif cols[col]["type"] in ["blob", "geometry", "json", "text"]:
                    col_opts.append(f"DEFAULT ({col_default})")
                elif col_default == "now()":
                    col_opts.append("DEFAULT NOW() ON UPDATE NOW()")
                elif col_default == "static_now()":
                    col_opts.append("DEFAULT NOW()")
                else:
                    col_opts.append(f"DEFAULT {col_default}")
            if col_attributes.get("invisible"):
                col_opts.append("INVISIBLE")
            if col_attributes.get("auto_increment"):
                col_opts.append("AUTO_INCREMENT")
                auto_inc_exists = True

            col_defs[
                col
            ] = f"  `{col}` {cols[col]['type']}{' ' + ' '.join(col_opts) if col_opts else ''},"
        msg += "\n".join(col_defs.values())
        msg += f"\n  PRIMARY KEY (`{pk}`){',' if uniques else ''}\n"
        for i, u in enumerate(uniques, 1):
            msg += f"  UNIQUE KEY {u} (`{u}`)"
            if not i == len(uniques) and len(uniques) > 1:
                msg += ",\n"
            else:
                msg += "\n"
        msg += f") ENGINE=InnoDB {'AUTO_INCREMENT=0' if auto_inc_exists else ''} DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
        return (msg, cols)
