from collections import defaultdict
from datetime import datetime, timedelta
from math import floor
import random

from utilities import utilities


class Generator:
    def __init__(self, args):
        self.args = args
        self.utils = utilities.Utilities()
        self.start_date = datetime(1995, 5, 23)
        self.end_date = datetime.now()

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
                    case "is_numeric_array":
                        pass
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
