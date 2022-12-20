import sys
sys.path.append('../build/')
import kuzu

databaseDir = "path to database file"
db = kuzu.Database(databaseDir)
conn = kuzu.Connection(db)
query = "MATCH (a:person) RETURN *;"
result = conn.execute(query)
while result.has_next():
    print(result.get_next())
result.close()
