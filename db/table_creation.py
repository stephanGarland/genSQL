import sqlite3
from contextlib import closing

conn = sqlite3.connect("db/gensql.db", isolation_level=None)

### NAMES ###

name_create = """
CREATE TABLE IF NOT EXISTS person_name (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL
) STRICT;
"""

lorem_create = """
CREATE TABLE IF NOT EXISTS lorem (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  paragraph TEXT NOT NULL
) STRICT;
"""

word_create = """
CREATE TABLE IF NOT EXISTS word (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  word TEXT NOT NULL
) STRICT;
"""

### GEO ###

country_create = """
CREATE TABLE IF NOT EXISTS country (
  code_country TEXT NOT NULL,
  code_country_ext TEXT NOT NULL,
  code_country_num TEXT NOT NULL,
  name TEXT NOT NULL,
  CONSTRAINT country_pk
    PRIMARY KEY (code_country)
) STRICT, WITHOUT rowid;
"""

province_create = """
CREATE TABLE IF NOT EXISTS province (
  code_country TEXT NOT NULL,
  code_sub TEXT NOT NULL,
  name TEXT NOT NULL,
  CONSTRAINT province_pk PRIMARY KEY (code_country, code_sub),
  CONSTRAINT province_code_country_code_fk
    FOREIGN KEY (code_country) REFERENCES country (code_country)
) STRICT, WITHOUT rowid;
"""

# Ideally, this table would have this FK:
#  CONSTRAINT state_fips_code_fips_code_state_fk
#    FOREIGN KEY (code_fips) REFERENCES fips (code_state)
# But since there are some without a FIPS code (AFAIK), like American Samoa,
# it's not feasible at the moment
state_us_create = """
CREATE TABLE IF NOT EXISTS state_us (
  code_country TEXT NOT NULL DEFAULT 'US',
  code_state TEXT NOT NULL,
  abbrev TEXT NULL,
  code_fips TEXT NULL,
  name TEXT NOT NULL,
  CONSTRAINT state_pk
    PRIMARY KEY (code_state)
) STRICT, WITHOUT rowid;
"""

state_create = """
CREATE TABLE IF NOT EXISTS state (
  code_state TEXT NOT NULL,
  code_country TEXT NOT NULL,
  name TEXT NOT NULL,
  CONSTRAINT state_pk
    PRIMARY KEY (code_country, code_state),
  CONSTRAINT state_country_code_country_code_fk
    FOREIGN KEY (code_country) REFERENCES country(code_country)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) STRICT, WITHOUT rowid;
"""

postcode_create = """
CREATE TABLE IF NOT EXISTS postcode (
  code_post TEXT NOT NULL,
  code_country TEXT NOT NULL,
  code_state TEXT NULL,
  CONSTRAINT postcode_pk
    PRIMARY KEY (code_country, code_post),
  CONSTRAINT postcode_country_code_country_code_fk
    FOREIGN KEY (code_country) REFERENCES country (code_country),
  CONSTRAINT postcode_state_code_state_code
    FOREIGN KEY (code_state) REFERENCES state(code_state)
) STRICT;
"""


postcode_us_create = """
CREATE TABLE IF NOT EXISTS postcode_us (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  code_post TEXT NOT NULL,
  code_state TEXT NOT NULL,
  city TEXT NOT NULL,
  CONSTRAINT postcode_us_code_state_us_code_state
    FOREIGN KEY (code_state) REFERENCES state_us (code_state)
) STRICT;
"""

city_create = """
CREATE TABLE IF NOT EXISTS city (
  code_country TEXT NOT NULL,
  city TEXT NOT NULL,
  CONSTRAINT city_pk
    PRIMARY KEY (code_country, city)
  CONSTRAINT city_country_code_country_id
    FOREIGN KEY (code_country) REFERENCES country(code_country)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) STRICT;
"""

fips_create = """
CREATE TABLE IF NOT EXISTS fips (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  code_state TEXT NOT NULL,
  code_county TEXT NULL,
  code_subdiv TEXT NULL,
  code_place TEXT NULL,
  code_consol_city TEXT NULL,
  name TEXT NOT NULL
) STRICT;
"""

city_indices = [
    "CREATE INDEX city_city_idx ON city (city)",
]

country_indices = [
    "CREATE UNIQUE INDEX country_name_unq_idx ON country (name)",
    "CREATE UNIQUE INDEX country_code_ext_unq_idx ON country (code_country_ext)",
    "CREATE UNIQUE INDEX country_code_num_unq_idx ON country (code_country_num)",
]

fips_indices = [
    "CREATE INDEX fips_code_state_idx ON fips (code_state)",
    "CREATE INDEX fips_code_name_idx ON fips (name)",
]

postcode_indices = [
    "CREATE INDEX postcode_code_state_idx ON postcode (code_state)",
]

postcode_us_indices = [
    "CREATE INDEX postcode_us_code_state_idx ON postcode_us (code_state)",
    "CREATE INDEX postcode_us_code_post_idx ON postcode_us (code_post)",
    "CREATE INDEX postcode_us_city_idx ON postcode_us (city)",
]

state_indices = [
    "CREATE INDEX state_code_state_idx ON state (code_state)",
    "CREATE INDEX state_name_idx ON state (name)",
]

us_state_indices = [
    "CREATE INDEX state_us_code_fips_idx ON state_us (code_fips)",
    "CREATE INDEX state_us_code_state_idx ON state_us (code_state)",
    "CREATE INDEX state_us_name_idx ON state_us (name)",
]

# TODO: are these necessary, since everything gets scanned once in bulk into shmem?
all_indices = (
    city_indices
    + country_indices
    + fips_indices
    + postcode_indices
    + postcode_us_indices
    + state_indices
    + us_state_indices
)


def return_rows(f, delim: str) -> list:
    if not delim in ["\t", ","]:
        raise ValueError(f"ERROR: delimiter {delim} not recognized")
    return [line.strip().split(delim) for line in f][1:]


with closing(conn):
    conn.execute("BEGIN")
    conn.execute(name_create)
    conn.execute(lorem_create)
    conn.execute(word_create)
    conn.execute(country_create)
    conn.execute(state_us_create)
    conn.execute(state_create)
    conn.execute(city_create)
    conn.execute(postcode_create)
    conn.execute(postcode_us_create)
    conn.execute(fips_create)
    conn.execute(province_create)
    conn.execute("COMMIT")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("BEGIN")

    with open("./content/first_names.txt", "r") as first_names, open(
        "./content/last_names.txt", "r"
    ) as last_names:
        f_lines = first_names.read().splitlines()
        l_lines = last_names.read().splitlines()
    min_len = min(len(f_lines), len(l_lines))
    f_lines = sorted(f_lines[:min_len])
    l_lines = sorted(l_lines[:min_len])
    rows = list(zip(f_lines, l_lines))
    conn.executemany(
        "INSERT INTO person_name (first_name, last_name) VALUES (?, ?)", rows
    )

    with open("./content/words.txt", "r") as f:
        rows = [(word,) for word in f.read().splitlines()]
        conn.executemany("INSERT INTO word (word) VALUES (?)", rows)

    with open("./content/lorem_ipsum.txt", "r") as f:
        rows = [(paragraph,) for paragraph in f.read().splitlines()]
        conn.executemany("INSERT INTO lorem (paragraph) VALUES (?)", rows)

    with open("./content/iso-3166-1.tsv", "r") as f:
        rows = return_rows(f, "\t")
        conn.executemany("INSERT INTO country VALUES (?, ?, ?, ?)", rows)

    with open("./content/iso-3166-2.tsv", "r") as f:
        rows = return_rows(f, "\t")
        conn.executemany("INSERT INTO province VALUES (?, ?, ?)", rows)

    with open("./content/fips_code.tsv", "r") as f:
        rows = return_rows(f, "\t")
        conn.executemany(
            "INSERT INTO fips (code_state, code_county, code_subdiv, code_place, code_consol_city, name) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )

    with open("./content/state_code.csv", "r") as f:
        rows = return_rows(f, ",")
        conn.executemany(
            "INSERT INTO state_us (code_state, abbrev, code_fips, name) VALUES (?, ?, ?, ?)",
            rows,
        )

    with open("./content/zip_code.csv", "r") as f:
        rows = return_rows(f, ",")
        conn.executemany(
            "INSERT INTO postcode_us (code_post, code_state, city) VALUES (?, ?, ?)",
            rows,
        )

    for idx in all_indices:
        conn.execute(idx)
    conn.execute("COMMIT")
