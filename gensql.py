from pathlib import PurePath
from textwrap import dedent
import sys

from gensql.generator import Generator
from gensql.runner import Runner
from gensql.validator import Validator

from exceptions.exceptions import (
    OutputFilePermissionError,
    OverwriteFileError,
)

from utilities import utilities

if __name__ == "__main__":
    warnings = []
    utils = utilities.Utilities()
    h = utilities.Help()
    args = utilities.Args().make_args()
    v = Validator(args)
    g = Generator(args)
    if not args.debug:
        sys.tracebacklimit = 0
    if args.extended_help:
        h.extended_help()
    elif args.validate:
        if v.validate_schema(v.parse_schema()):
            # \u2704 == green checkmark
            print("\u2705 INFO: validated schema, no errors detected")
            raise SystemExit(0)
    elif args.generate:
        # due to the multi-line string output, there's a leading newline to remove
        skeleton = dedent(h.make_skeleton())[1:]
        try:
            try:
                filename = PurePath(args.output).stem
            except TypeError:
                filename = "skeleton"
            with open(f"{filename}.json", f"{'w' if args.force else 'x'}") as f:
                f.write(skeleton)
            raise SystemExit(0)
        except FileExistsError:
            raise OverwriteFileError(filename) from None
        except PermissionError:
            raise OutputFilePermissionError(filename) from None
    elif args.dates:
        if args.output:
            warnings.append(f"--output {args.output}")
        if args.filetype:
            warnings.append(f"--filetype {args.filetype}")

        try:
            filename = "content/dates.txt"
            with open(filename, f"{'w' if args.force else 'x'}") as f:
                dates = [x + "\n" for x in g.make_dates(args.num)]
                f.writelines(dates)
            if warnings:
                for w in warnings:
                    print(f"INFO: ignoring option {w} for datetime creation")
            print(f"INFO: {args.num} datetimes created at content/dates.txt")
            raise SystemExit(0)
        except FileExistsError:
            raise OverwriteFileError(filename) from None
        except PermissionError:
            raise OutputFilePermissionError(filename) from None
    tbl_name = args.table or args.output or "gensql"
    schema_dict = v.parse_schema()
    schema_dict = utils.lowercase_schema(schema_dict)
    v.validate_schema(schema_dict)
    tbl_create, tbl_cols = g.mysql(schema_dict, tbl_name, args.drop_table)
    # generally, there isn't a good reason to insert values manually into an auto-incrementing col
    auto_inc_cols = [x for x in tbl_cols.keys() if tbl_cols[x].get("auto_inc")]
    for x in auto_inc_cols:
        del tbl_cols[x]
    r = Runner(args, schema_dict, tbl_name, tbl_cols, tbl_create)
    r.run()
