import sqlite3

conn = sqlite3.connect("gensql.db")
cursor = conn.cursor()

cities_create = """
CREATE TABLE cities (
  city TEXT NOT NULL,
  country TEXT NOT NULL,
  PRIMARY KEY (city, country)
) WITHOUT rowid;
"""

countries_create = """
CREATE TABLE countries (
  code TEXT PRIMARY KEY NOT NULL,
  country TEXT NOT NULL
);
"""

names_create = """
CREATE TABLE names (
  id INTEGER PRIMARY KEY NOT NULL,
  first_names TEXT NOT NULL,
  last_names TEXT NOT NULL
);
"""

words_create = """
CREATE TABLE words (
  id INTEGER PRIMARY KEY NOT NULL,
  word TEXT NOT NULL
);
"""

tables = ["cities", "countries", "names", "words"]

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
    cursor.execute("INSERT INTO cities (city, country) VALUES (?, ?)", (line.split(",")[0].strip(), line.split(",")[1].strip()))
    conn.commit()

with open("../content/country_codes.txt", "r") as f:
    lines = f.read().splitlines()
for line in lines:
    cursor.execute("INSERT INTO countries (country, code) VALUES (?, ?)", (line.split(",")[0].strip(), line.split(",")[1].strip()))
    conn.commit()

with open("../content/first_names.txt", "r") as first_names, open("../content/last_names.txt", "r") as last_names:
    f_lines = first_names.read().splitlines()
    l_lines = last_names.read().splitlines()
    lines = list(zip(f_lines, l_lines))
for line in lines:
    cursor.execute("INSERT INTO names (first_names, last_names) VALUES (?, ?)", (line[0], line[1]))
    conn.commit()

with open("../content/wordlist.txt", "r") as f:
    lines = f.read().splitlines()
for line in lines:
    cursor.execute("INSERT INTO words (word) VALUES (?)", (line,))
    conn.commit()

conn.close()
