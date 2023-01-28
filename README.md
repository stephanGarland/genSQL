# GenSQL

## Overview

Ever want to create millions of rows of random data for a database, with realistic names, datetimes, etc.? Now you can.

## Usage

```shell
usage: create_entries.py [-h] [--extended-help] [-d] [-f {csv,mysql,postgres,sqlserver,txt}] [-g] [-i INPUT] [-n NUM] [-o OUTPUT] [-t TABLE]

options:
  -h, --help            show this help message and exit
  --extended-help       Print extended help
  -d, --generate-dates  Generate a file of datetimes for later use
  -f {csv,mysql,postgres,sqlserver,txt}, --filetype {csv,mysql,postgres,sqlserver,txt}
                        Filetype to generate
  -g, --generate-skeleton
                        Generate a skeleton input JSON schema
  -i INPUT, --input INPUT
                        Input schema (JSON) to generate data for
  -n NUM, --num NUM     The number of rows to generate
  -o OUTPUT, --output OUTPUT
                        Output filename
  -t TABLE, --table TABLE
                        Table name to generate SQL for
```

## Notes

* The `-f` flag does absolutely nothing right now.
* There is limited validation of a given input schema.
* The `-d` flag works, but using said file is not currently implemented (it wasn't much faster anyway).
* If you're creating more than ~100K rows, installing `numpy` will provide a small speed boost.
* There is no guarantee that the datetimes are actually valid; specifically, this can create datetimes that occurred during a springtime DST event.

## TODO

* Speed it up. Right now, with the default skeleton schema, it takes ~5 minutes to generate 1,000,000 rows on my server, with spinning disks.
  * The primary slowdown is `shuffle` for the random ID allocations.
* Fix the invalid datetime creation.
* Support other SQL varieties, as well as CSV and TXT.
* Improve input schema validation.
* Create tests.

