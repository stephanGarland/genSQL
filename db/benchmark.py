import sqlite3
import time

conn = sqlite3.connect("gensql.db")

cursor = conn.cursor()
times = {}
# q_rowid = f"SELECT c.city, c.country, cc.code FROM {city} c JOIN {country} cc ON c.country = cc.country"
q_subquery = "SELECT c.city, c.country FROM cities c WHERE c.country = (SELECT cc.country FROM countries cc WHERE cc.code = 'US')"
q_join = "SELECT c.city, c.country FROM cities c JOIN countries cc ON c.country = cc.country WHERE cc.code = 'US'"

# only used when benchmarking table schema
# for _ in range(100):
#    for city in ["cities", "cities_no_rowid"]:
#        for country in ["countries", "countries_no_rowid"]:
#            start_time = time.time()
#            cursor.execute(q_rowid)
#            records = cursor.fetchall()
#            end_time = time.time()
#            times[end_time - start_time] = f"{city} JOIN {country}"

for _ in range(1000):
    start_time = time.time()
    cursor.execute(q_subquery)
    records = cursor.fetchall()
    end_time = time.time()
    times[end_time - start_time] = "subquery"
    start_time = time.time()
    cursor.execute(q_join)
    records = cursor.fetchall()
    end_time = time.time()
    times[end_time - start_time] = "where"
sorted_keys = sorted(times.keys())

print("fastest 5")
for i in range(5):
    print(f"{i}: {times[sorted_keys[i]]} - {sorted_keys[i]}")
print("slowest 5")
for i in range(-1, -6, -1):
    print(f":{i}:  {times[sorted_keys[i]]} - {sorted_keys[i]}")
conn.close()

# table schema benchmarks
# M1 Air
# shortest: cities_no_rowid JOIN countries - 0.004974842071533203
# longest: cities_no_rowid JOIN countries_no_rowid - 0.10073494911193848

# Xeon CPU E5-2650 v2
# shortest: cities_no_rowid JOIN countries - 0.01211094856262207
# longest: cities_no_rowid JOIN countries_no_rowid - 0.4228515625
