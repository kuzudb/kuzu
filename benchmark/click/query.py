#!/usr/bin/env python3

import timeit
import sys

import kuzu

query = sys.stdin.read()
print(query)

db = kuzu.Database(":memory:")
con = kuzu.Connection(db)
ver = con.execute("call DB_Version() return *;").get_next()[0]
db = kuzu.Database(f"mydb-{ver}", read_only=True)
con = kuzu.Connection(db)
for try_num in range(3):
    start = timeit.default_timer()
    results = con.execute(query.replace('\\', '\\\\'))
    end = timeit.default_timer()
    print(end - start)
