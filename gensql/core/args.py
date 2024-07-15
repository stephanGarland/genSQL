import argparse


def make_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--extended-help",
        action="store_true",
        dest="extended_help",
        help="Print extended help",
    )
    parser.add_argument(
        "--country",
        choices=["random", "au", "de", "fr", "gb", "ke", "jp", "mx", "ua", "us"],
        default="random",
        help="A specific country (or random) to use for cities, phone numbers, etc.",
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Print tracebacks for errors"
    )
    parser.add_argument(
        "--drop-table",
        action="store_true",
        dest="drop_table",
        help="WARNING: DESTRUCTIVE - use DROP TABLE with generation",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="WARNING: DESTRUCTIVE - overwrite any files",
    )
    parser.add_argument(
        "-f",
        "--filetype",
        choices=["csv", "mysql", "postgres"],
        default="mysql",
        help="Filetype to generate",
    )
    parser.add_argument(
        "--fixed-length",
        action="store_true",
        dest="fixed_length",
        help="Disable any variations in length for JSON arrays, text, etc.",
    )
    parser.add_argument(
        "--generate-dates",
        action="store_true",
        dest="dates",
        help="Generate a file of datetimes for later use",
    )
    parser.add_argument(
        "-g",
        "--generate-skeleton",
        action="store_true",
        dest="generate",
        help="Generate a skeleton input JSON schema",
    )
    parser.add_argument("-i", "--input", help="Input schema (JSON)")
    parser.add_argument(
        "--no-check",
        action="store_true",
        dest="no_check",
        help="Do not perform validation checks for unique columns",
    )
    parser.add_argument(
        "--no-chunk",
        action="store_true",
        dest="no_chunk",
        help="Do not chunk SQL INSERT statements",
    )
    parser.add_argument(
        "-n",
        "--num",
        type=int,
        default=1000,
        help="The number of rows to generate - defaults to 1000",
    )
    parser.add_argument("-o", "--output", help="Output filename - defaults to gensql")
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress printing various informational messages",
    )
    parser.add_argument(
        "-r",
        "--random",
        action="store_true",
        help="Enable randomness on the length of some items",
    )
    parser.add_argument(
        "-t",
        "--table",
        help="Table name to generate SQL for - defaults to the filename",
    )
    parser.add_argument("--validate", help="Validate an input JSON schema")
    return parser.parse_args()
