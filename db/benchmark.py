import sqlite3
import time

conn = sqlite3.connect("gensql.db")

cursor = conn.cursor()
times = {}

for _ in range(100):
    for city in ["cities", "cities_no_rowid"]:
        for country in ["countries", "countries_no_rowid"]:
            start_time = time.time()
            cursor.execute(f"SELECT c.city, c.country, cc.code FROM {city} c JOIN {country} cc ON c.country = cc.country")
            records = cursor.fetchall()
            end_time = time.time()
            times[end_time - start_time] = f"{city} JOIN {country}"

sorted_keys = sorted(times.keys())

print(f"shortest: {times[sorted_keys[0]]} - {sorted_keys[0]}")
print(f"longest: {times[sorted_keys[-1]]} - {sorted_keys[-1]}")
conn.close()

# M1 Air
# shortest: cities_no_rowid JOIN countries - 0.004974842071533203
# longest: cities_no_rowid JOIN countries_no_rowid - 0.10073494911193848

# Xeon CPU E5-2650 v2
# shortest: cities_no_rowid JOIN countries - 0.01211094856262207
# longest: cities_no_rowid JOIN countries_no_rowid - 0.4228515625
