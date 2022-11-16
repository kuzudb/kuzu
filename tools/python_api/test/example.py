from tools.python_api import _kuzu as kuzu

databaseDir = "path to database file"
db = kuzu.database(databaseDir)
conn = kuzu.connection(db)
query = "MATCH (a:person) RETURN *;"
result = conn.execute(query)
while result.hasNext():
    print(result.getNext())
result.close()
