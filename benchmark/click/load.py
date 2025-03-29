#!/usr/bin/env python3

import kuzu
import timeit
import psutil

db = kuzu.Database("mydb")
con = kuzu.Connection(db)

start = timeit.default_timer()
con.execute(open("create.cypher").read())
con.execute("COPY hits FROM 'hits.csv' (PARALLEL=false);")
end = timeit.default_timer()
print(end - start)
