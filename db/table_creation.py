import sqlite3

conn = sqlite3.connect("gensql_new.db")
cursor = conn.cursor()

cities_create = """
CREATE TABLE city (
  city TEXT NOT NULL,
  country TEXT NOT NULL,
  PRIMARY KEY (city, country)
) WITHOUT rowid;
"""

countries_create = """
CREATE TABLE country (
  code TEXT PRIMARY KEY NOT NULL,
  country TEXT NOT NULL
);
"""

names_create = """
CREATE TABLE person_name (
  id INTEGER PRIMARY KEY NOT NULL,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL
);
"""

words_create = """
CREATE TABLE word (
  id INTEGER PRIMARY KEY NOT NULL,
  word TEXT NOT NULL
);
"""

tables = ["city", "country", "person_name", "word"]

table_create = []
table_create.append(cities_create)
table_create.append(countries_create)
table_create.append(names_create)
table_create.append(words_create)

for table in tables:
    cursor.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()

for create in table_create:
    cursor.execute(create)
    conn.commit()

with open("../content/cities_countries.txt", "r") as f:
    lines = f.read().splitlines()
for line in lines:
    cursor.execute(
        "INSERT INTO city (city, country) VALUES (?, ?)",
        (line.split(",")[0].strip(), line.split(",")[1].strip()),
    )
    conn.commit()

with open("../content/country_codes.txt", "r") as f:
    lines = f.read().splitlines()
for line in lines:
    cursor.execute(
        "INSERT INTO country (country, code) VALUES (?, ?)",
        (line.split(",")[0].strip(), line.split(",")[1].strip()),
    )
    conn.commit()

with open("../content/first_names.txt", "r") as first_names, open(
    "../content/last_names.txt", "r"
) as last_names:
    f_lines = first_names.read().splitlines()
    l_lines = last_names.read().splitlines()
min_len = (min(len(f_lines), len(l_lines)))
f_lines = sorted(f_lines[:min_len])
l_lines = sorted(l_lines[:min_len])
lines = list(zip(f_lines, l_lines))
for line in lines:
    cursor.execute(
        "INSERT INTO person_name (first_name, last_name) VALUES (?, ?)", (line[0], line[1])
    )
    conn.commit()

with open("../content/wordlist.txt", "r") as f:
    lines = f.read().splitlines()
for line in lines:
    cursor.execute("INSERT INTO word (word) VALUES (?)", (line,))
    conn.commit()

conn.close()
