import json
from pathlib import PurePath

from exceptions.exceptions import SchemaValidationError
from utilities.constants import ALLOWED_COLS, ALLOWED_UNIQUES, MYSQL_INT_MIN_MAX
from utilities import utilities


class Validator:
    def __init__(self, args):
        self.args = args
        self.utils = utilities.Utilities()

    def show_json_error(self, schema_filename: str | PurePath, e: json.JSONDecodeError):
        """
        Attempts to show the location in an input schema where
        a JSONDecodeError occurred.
        """

        _json_property_err = False
        _json_delimiter_err = False

        err_msg = "ERROR: unable to parse JSON - "
        # probable cause is a trailing comma after the last item
        if "property name" in e.msg:
            _json_property_err = True
            err_msg += "check for trailing commas"
        # probable cause is a missing colon delimiter between k:v
        elif "delimiter" in e.msg:
            _json_delimiter_err = True
            err_msg += "check for missing colons"
        print(err_msg)
        with open(schema_filename, "r") as f:
            err_json = f.read().splitlines()
        for i, line in enumerate(err_json, start=1):
            if i == e.lineno - 1:
                if _json_property_err:
                    # \u274c == red cross mark
                    print(f"{i:03}: {line} \u274c")
            elif i == e.lineno and _json_delimiter_err:
                print(f"{i:03}: {line} \u274c")
            else:
                print(f"{i:03}: {line}")
        raise SystemExit(1)

    def parse_schema(self) -> dict[str, dict[str, str]]:
        """
        Parses input schema in JSON format and returns
        a dictionary of the required columns and their types.

        Does not perform any validation that the requested
        column names or types are valid.
        """

        try:
            if self.args.input:
                filename = PurePath(self.args.input).with_suffix(".json")
            elif self.args.validate:
                filename = PurePath(self.args.validate).with_suffix(".json")
            else:
                filename = "schema_inputs/skeleton.json"
            with open(filename, "r") as f:
                try:
                    schema = json.loads(f.read())
                except json.JSONDecodeError as e:
                    self.show_json_error(filename, e)
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
            col_json_num_arr = v.get("is_numeric_array")
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
                    f"column `{k}` must be of type `char` or `varchar`",
                )
            if col_unique and k not in ALLOWED_UNIQUES and "int" not in col_type:
                _add_error(
                    errors,
                    (k, "unique"),
                    v,
                    f"unique is not a valid option for column `{k}`",
                )
            if not col_type:
                _add_error(
                    errors,
                    (k, "type"),
                    v,
                    f"column `{k}` is missing a type property",
                )
            if col_type not in ALLOWED_COLS:
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
            if col_width and "char" not in col_type and "binary" not in col_type:
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
            if col_json_num_arr and not col_type == "json":
                _add_error(
                    errors,
                    (k, "is_numeric_array"),
                    v,
                    f"is_numeric_array is not a valid option for column `{k}` of type `{col_type}`",
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
