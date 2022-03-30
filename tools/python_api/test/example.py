from tools.python_api import _graphflowdb as gdb


databaseDir = "path_to_serialized_database"
db = gdb.database(databaseDir)
conn = gdb.connection(db)
query = "MATCH (a:person) RETURN *;"
result = conn.query(query)
while result.hasNext():
    print(result.getNext())
result.close()
