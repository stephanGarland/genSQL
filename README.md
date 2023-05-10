# GenSQL

## Overview

Ever want to quickly create millions of rows of random data for a database, with realistic names, datetimes, etc.? Now you can.

## Usage

```shell
usage: gensql.py [-h] [--extended-help] [--country {random,au,de,fr,gb,ke,jp,mx,ua,us}] [-d] [--drop-table] [--force] [-f {csv,mysql,postgresql,sqlserver}] [--fixed-length]
                 [--generate-dates] [-g] [-i INPUT] [--no-check] [--no-chunk] [-n NUM] [-o OUTPUT] [-q] [-r] [-t TABLE] [--validate VALIDATE]

options:
  -h, --help            show this help message and exit
  --extended-help       Print extended help
  --country {random,au,de,fr,gb,ke,jp,mx,ua,us}
                        A specific country (or random) to use for cities, phone numbers, etc.
  -d, --debug           Print tracebacks for errors
  --drop-table          WARNING: DESTRUCTIVE - use DROP TABLE with generation
  --force               WARNING: DESTRUCTIVE - overwrite any files
  -f {csv,mysql,postgresql,sqlserver}, --filetype {csv,mysql,postgresql,sqlserver}
                        Filetype to generate
  --fixed-length        Disable any variations in length for JSON arrays, text, etc.
  --generate-dates      Generate a file of datetimes for later use
  -g, --generate-skeleton
                        Generate a skeleton input JSON schema
  -i INPUT, --input INPUT
                        Input schema (JSON)
  --no-check            Do not perform validation checks for unique columns
  --no-chunk            Do not chunk SQL INSERT statements
  -n NUM, --num NUM     The number of rows to generate - defaults to 1000
  -o OUTPUT, --output OUTPUT
                        Output filename - defaults to gensql
  -q, --quiet           Suppress printing various informational messages
  -r, --random          Enable randomness on the length of some items
  -t TABLE, --table TABLE
                        Table name to generate SQL for - defaults to the filename
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
3. Run GenSQL, example `python3 gensql.py -i $YOUR_SCHEMA.json -n 10000 -f mysql`.

## Requirements

* A C compiler that's reasonably new, if the included options (`linux-x86_64`, `darwin-arm64`, and `darwin-x86_64`) don't work for you.
* Python >= 3.10, but 3.11 is recommended, as it's 15-20% faster for this application.

## Notes

* The `--filetype` flag only supports `csv` and `mysql`. The only supported RDBMS is MySQL (probably 8.x; it _might_ work with 5.7.8 if you want a JSON column, and earlier if you don't).
* Generated datetimes are in UTC, i.e. no DST events exist. If you remove the query to set the session's timezone, you may have a bad time.
* This uses a C library for a few functions, notably filling large arrays and shuffling them. For UUID creation, the library <uuid/uuid.h> is required to build the shared library.
* Currently, generating UUIDs only supports v1 and v4, and if they're to be stored as `BINARY` types, only .sql file format is supported. Also as an aside, it's a terrible idea to use a UUID (at least v4) as a PK in InnoDB, so please be sure of what you're doing. If you don't believe me, generate one, and another using a monotonic integer or something similar, and compare on-disk sizes for the tablespaces.
* `--force` and `--drop-table` have warnings for a reason. If you run a query with `DROP TABLE IF EXISTS`, please be sure of what you're doing.
* `--random` allows for TEXT and JSON columns to have varying amounts of length, which may or may not matter to you. It will cause a ~10% slowdown. If not selected, a deterministic 20% of the rows in these columns will have a longer length than the rest. If this also bothers you, use `--fixed-length`.
* `--generate-dates` takes practically the same amount of time, or slightly longer, than just having them generated on-demand. It's useful if you want to have the same set of datetimes for a series of tables, although their actual ordering for row generation will remain random.
* Any column with `id` in its name will by default be assumed to be an integer type, and will have integers generated for it. You can provide hints to disable this, or to enable it for columns without `id` in their names, by using `is_id: {true, false}` in your schema.
* To have an empty JSON array be set as the default value for a JSON column, use the default value `array()`.
* The generated values for a JSON column can be an object of random words (the default), or an array of random integers. For the latter, set the hint `is_numeric_array` in the schema's object.
* To have a column be given no `INSERT` statements, e.g. remain empty / with its default value, set the hint `is_empty: true` in the schema definition for the column.
* To have the current datetime statically defined as the default value for a TIMESTAMP column, use the default value `static_now()`. To also have the column's default automatically update the timestamp, use the default value `now()`. To have the column's default value be NULL, but update automatically to the current timestamp when the row is updated, use `null_now()`.
* Using a column of name `phone` will generate realistic - to the best of my knowledge - phone numbers for a given country (very limited set). It's currently non-optimized for performance, and thus incurs a ~40% slowdown over the baseline. A solution in C may or may not speed things up, as it's not that performing `random.shuffle()` on a 10-digit number is slow, it's that doing so `n` times is a lot of function calls. Inlining C functions in Python [does exist](https://github.com/ssize-t/inlinec), but the non-caching of its compilation would probably negate any savings.
* Similarly, a column of name `email` will generate realistic email addresses (all with `.com` TLD), and will incur a ~40% slowdown over the baseline.

### Loading data

For MySQL, if you have access to the host (i.e. not DBaaS), by far the fastest method to load data is by using [LOAD DATA INFILE](https://dev.mysql.com/doc/refman/8.0/en/load-data.html) from a CSV file.
To do this, you first need to create the table. When you specify `--filetype csv`, GenSQL generates a table definition separately from the data CSV, named `tbl_create.sql`. You can use the `mysql` client to create the table like so:

```shell
mysql -h $HOST -u $USER -p $SCHEMA < tbl_create.sql
```

And then, from within the `mysql` client:

```mysql
mysql> SET @@time_zone = '+00:00';
mysql> SET @@unique_checks = 0;
mysql> LOAD DATA INFILE '/path/to/your/file.csv' INTO TABLE $TABLE_NAME FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY "'" IGNORE 1 LINES ($COL_0, $COL_1, ... $COL_N);
Query OK, 1000 rows affected (1.00 sec)
Records: 1000  Deleted: 0  Skipped: 0  Warnings: 0
```

If free space on the host is an issue, and you have an extremely large file, you can compress the file and then pipe it through a named pipe to the database, like this:

```shell
mkfifo --mode=0644 /path/to/your/pipe
gzip -c -d sql_data.csv.gz > /path/to/your/pipe &
mysql -h $HOST -u $USER -p$PASS $SCHEMA -e "SET @@time_zone = '+00:00'; LOAD DATA INFILE '/path/to/your/pipe' INTO TABLE $TABLE_NAME FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY \"'\" IGNORE 1 LINES;"
```

Note that this will may be somewhat slower than simply decompressing the file and loading it.

However, if you don't have access to the host, there are some tricks GenSQL has to speed things up. Specifically:

* Disabling autocommit
  * Normally, each statement is committed one at a time. With this disabled, an explicit `COMMIT` statement must be used to commit.
* Disabling unique checks
  * Normally, the SQL engine will check that any columns declaring `UNIQUE` constraints do in fact meet that constraint. With this disabled, repetitive `INSERT` statements are much faster, with the obvious risk of violating the constraint. Since GenSQL by default does its own checks at creation for unique columns (currently limited to integer columns and `email` columns), this is generally safe to disable. If you use `--no-check`, this should not be disabled.
* Multi-INSERT statements
  * Normally, an `INSERT` statement might look something like `INSERT INTO $TABLE (col_1, col_2) VALUES (row_1, row_2);` Instead, they can be written like `INSERT INTO $TABLE (col_1, col_2) VALUES (row_1, row_2), (row_3, row_4),` with `n` tuples of row data. By default, `mysqld` (the server) is limited to a 64 MiB packet size, and `mysql` (the client) to a 16 MiB packet size. Both of these can be altered up to 1 GiB, but the server side may not be accessible to everyone, so GenSQL limits itself to a 10,000 row chunk size, which should comfortably fit under the server limit. For the client, you'll need to pass `--max-allowed-packet=67108864` as an arg. If you don't want this behavior, you can use `--no-chunk` when creating the data.


Testing with inserting 100,000 rows (DB is backed by spinning disks):

```shell
# autocommit and unique checks disabled, but no chunking
❯ time mysql -h localhost -usgarland -ppassword test < normal.sql
mysql -h localhost -usgarland -ppassword test < normal.sql  34.55s user 11.48s system 14% cpu 5:15.65 total

# autocommit and unique checks disabled, chunking into 10,000 INSERTs
❯ time mysql -h localhost -usgarland -ppassword test --max-allowed-packet=67108864 < chunk.sql
mysql -h localhost -usgarland -ppassword test --max-allowed-packet=67108864 < chunk.sql 10.74s user 0.21s system 9% cpu 1:50.68 total

# LOAD DATA INFILE
❯ time mysql -h 127.0.0.1 -usgarland -ppassword test -e "SET @@time_zone = '+00:00'; LOAD DATA INFILE '/mnt/ramdisk/test.csv' INTO TABLE gensql FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY \"'\" IGNORE 1 LINES;"
mysql -h 127.0.0.1 -usgarland -ppassword test -e   0.02s user 0.01s system 0% cpu 1:19.99 total
```

Or, in terms of ratios, using chunking is approximately 3x as fast as the baseline, while loading a CSV is approximately 4x as fast as the baseline.

```
# baseline
❯ time mysql -h localhost -usgarland -ppassword test < test.sql
mysql -h localhost -usgarland -ppassword test < test.sql  32.75s user 10.90s system 14% cpu 4:55.91 total
# no unique checks
❯ time mysql -h localhost -usgarland -ppassword test < test.sql
mysql -h localhost -usgarland -ppassword test < test.sql  25.11s user 8.67s system 14% cpu 3:48.38 total
# no unique checks, single insert, 1 gb buffer size
❯ time mysql -h localhost -usgarland -ppassword --max-allowed-packet=1073741824 test < test.sql
mysql -h localhost -usgarland -ppassword --max-allowed-packet=1073741824 test  10.64s user 0.91s system 7% cpu 2:28.29 total
```

## Benchmarks

**NOTE: THESE ARE NOT CURRENT, AND SHOULD NOT BE RELIED ON**

Testing the creation of the standard 4-column schema, as well as an extended 8-column schema, with 1,000,000 rows.

### M1 Macbook Air

#### Python 3.11

```shell
❯ time python3.11 gensql.py -n 1000000 --force --drop-table
python3.11 gensql.py -n 1000000 --force --drop-table  4.56s user 0.16s system 99% cpu 4.744 total

❯ time python3.11 gensql.py -i full.json -n 1000000 --force --drop-table
python3.11 gensql.py -i full.json -n 1000000 --force --drop-table  12.70s user 1.13s system 98% cpu 14.089 total
```

#### Python 3.10

```shell
❯ time python3 gensql.py -n 1000000 --force --drop-table
python3 gensql.py -n 1000000 --force --drop-table  5.27s user 0.17s system 99% cpu 5.442 total

❯ time python3 gensql.py -i full.json -n 1000000 --force --drop-table
python3 gensql.py -i full.json -n 1000000 --force --drop-table  16.23s user 0.54s system 99% cpu 16.840 total
```

### Intel i9  Macbook Pro

#### Python 3.11

```shell
❯ time python3.11 gensql.py -n 1000000 --force --drop-table
python3.11 gensql.py -n 1000000 --force --drop-table  8.51s user 0.47s system 99% cpu 9.023 total

❯ time python3.11 gensql.py -i full.json -n 1000000 --force --drop-table
python3.11 gensql.py -i full.json -n 1000000 --force --drop-table  25.68s user 1.60s system 99% cpu 27.395 total
```

#### Python 3.10

```shell
❯ time python3 gensql.py -n 1000000 --force --drop-table
python3 gensql.py -n 1000000 --force --drop-table  9.88s user 0.46s system 99% cpu 10.405 total

❯ time python3 gensql.py -i full.json -n 1000000 --force --drop-table
python3 gensql.py -i full.json -n 1000000 --force --drop-table  32.60s user 1.66s system 99% cpu 34.364 total
```

### Xeon E5-2650v2 server

A ramdisk was used to eliminate the spinning disk overhead for the server.

```shell
❯ time python3.11 gensql.py -n 1000000 --force --drop-table -o /mnt/ramdisk/test.sql
python3.11 gensql.py -n 1000000 --force --drop-table -o   15.35s user 0.85s system 98% cpu 16.377 total

❯ time python3.11 gensql.py -i full.json -n 1000000 --force --drop-table -o /mnt/ramdisk/test.sql
python3.11 gensql.py -i full.json -n 1000000 --force --drop-table -o   45.26s user 3.79s system 99% cpu 49.072 total
```

## TODO

* Support other SQL varieties.
* Add more column data sources.
* Create tests.
* Come up with a coherent exception handling mechanism.
* Add logging, maybe.
* Continue chasing speedups.
