# GenSQL

## Overview

Ever want to quickly create millions of rows of random data for a database, with realistic names, datetimes, etc.? Now you can.

## Usage

```shell
usage: create_entries.py [-h] [--extended-help] [-d] [--drop-table] [--force] [-f {csv,mysql,postgresql,sqlserver}] [--generate-dates] [-g] [-i INPUT] [-n NUM] [-o OUTPUT]
                         [-t TABLE] [--validate VALIDATE]

options:
  -h, --help            show this help message and exit
  --extended-help       Print extended help
  -d, --debug           Print tracebacks for errors
  --drop-table          WARNING: DESTRUCTIVE - use DROP TABLE with generation
  --force               WARNING: DESTRUCTIVE - overwrite any files
  -f {csv,mysql,postgresql,sqlserver}, --filetype {csv,mysql,postgresql,sqlserver}
                        Filetype to generate
  --generate-dates      Generate a file of datetimes for later use
  -g, --generate-skeleton
                        Generate a skeleton input JSON schema
  -i INPUT, --input INPUT
                        Input schema (JSON)
  -n NUM, --num NUM     The number of rows to generate
  -o OUTPUT, --output OUTPUT
                        Output filename
  -t TABLE, --table TABLE
                        Table name to generate SQL for
  --validate VALIDATE   Validate an input JSON schema
```

### Usage example

1. Create a schema if you'd like, or use the included examples.

```
GenSQL expects a JSON input schema, of the format:

            {
                "col_name": {
                    "col_type": "type",
                    "col_option_0": "option",
                    "col_option_n": "option"
                }
            }
```
2. If necessary, build the C library with the included Makefile. Otherwise, rename the included file for your platform to `fast_shuffle.so` (or change the name ctypes is looking for, your choice).
3. Run GenSQL, example `python3 create_entries.py -i $YOUR_SCHEMA.json -n 10000 -f mysql`.

## Requirements

* A C compiler that's reasonably new, if the included options (`linux-x86_64` and `darwin-arm64`) don't work for you.
* Python >= 3.10, but 3.11 is recommended, as it's 15-20% faster for this application.

## Notes

* The `-f` flag only supports `csv` and `mysql`. The only supported RDBMS is MySQL (probably 8.x; it _might_ work with 5.7.8 if you want a JSON column, and earlier if you don't).
* Generated datetimes are in UTC, i.e. no DST events exist. If you remove the query to set the session's timezone, you may have a bad time.
* This uses a C library to perform random shuffles. There are no external libraries, so as long as you have a reasonably new compiler, `make` should work for you.
* `--force` and `--drop-table` have warnings for a reason.

### Loading data

For MySQL, if you have access to the host (i.e. not DBaaS), by far the fastest method to load data is by using [LOAD DATA INFILE](https://dev.mysql.com/doc/refman/8.0/en/load-data.html). To do this, you first need to create the table. GenSQL generates a table definition separately from the data CSV, named `tbl_create.sql`. You can use the `mysql` client to create the table like so:

```shell
mysql -h $HOST -u $USER -p $SCHEMA < tbl_create.sql
```

And then, from within the `mysql` client:

```mysql
mysql> LOAD DATA INFILE '/path/to/your/file.csv' INTO TABLE $TABLE_NAME FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY "'" IGNORE 1 LINES;
Query OK, 1000 rows affected (1.00 sec)
Records: 1000  Deleted: 0  Skipped: 0  Warnings: 0
```

Otherwise, you can use the same method for `tbl_create.sql` for the entirety of the data load. It will be significantly slower, but with `autocommit=0` (set for you by default), it's manageable.

## Benchmarks

Testing the creation of the standard 4-column schema, as well as an extended 8-column schema, with 1,000,000 rows. These may not be precisely up to date, but actual results should be close to this if not faster.

### M1 Macbook Air

#### Python 3.11

```shell
❯ time python3.11 create_entries.py -n 1000000 --force --drop-table
python3.11 create_entries.py -n 1000000 --force --drop-table  4.56s user 0.16s system 99% cpu 4.744 total

❯ time python3.11 create_entries.py -i full.json -n 1000000 --force --drop-table
python3.11 create_entries.py -i full.json -n 1000000 --force --drop-table  12.70s user 1.13s system 98% cpu 14.089 total
```

#### Python 3.10

```shell
❯ time python3 create_entries.py -n 1000000 --force --drop-table
python3 create_entries.py -n 1000000 --force --drop-table  5.27s user 0.17s system 99% cpu 5.442 total

❯ time python3 create_entries.py -i full.json -n 1000000 --force --drop-table
python3 create_entries.py -i full.json -n 1000000 --force --drop-table  16.23s user 0.54s system 99% cpu 16.840 total
```

### Xeon E5-2650v2 server

A ramdisk was used to eliminate the spinning disk overhead for the server.

```shell
❯ time python3.11 create_entries.py -n 1000000 --force --drop-table -o /mnt/ramdisk/test.sql
python3.11 create_entries.py -n 1000000 --force --drop-table -o   15.35s user 0.85s system 98% cpu 16.377 total

❯ time python3.11 create_entries.py -i full.json -n 1000000 --force --drop-table -o /mnt/ramdisk/test.sql
python3.11 create_entries.py -i full.json -n 1000000 --force --drop-table -o   45.26s user 3.79s system 99% cpu 49.072 total
```

## TODO

* Support other SQL varieties, as well as CSV and TXT.
* Add more column data sources, like addresses, phone numbers, and email addresses.
* Create tests.
* Come up with a coherent exception handling mechanism.
* Add logging, maybe.
* Continue chasing speedups.
