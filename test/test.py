import tempfile
import sys

sys.path.append("/home/kuzu/kuzu/tools/python_api/build")

import json
import shutil
import kuzu

with tempfile.TemporaryDirectory() as dbpath:
    db = kuzu.Database(dbpath)
    conn = kuzu.Connection(db)

    queries = []
    with open("/home/kuzu/Downloads/G_47.json") as f:
        queries = json.load(f)

    for query in queries:
        conn.execute(query)

    query = "MATCH (n1)-[]-(n2:L1:L0) WHERE (n2.p53 <> n1.p43) OR (n2.p29 = n1.p14) RETURN COUNT (*)"
    res = conn.execute(query).get_as_pl()
    print(res)

# with tempfile.TemporaryDirectory() as dbpath:
#     conn = kuzu.Connection(kuzu.Database(dbpath))
#     conn.execute(
#         """
#         CREATE NODE TABLE V (id INT, PRIMARY KEY(id));
#         CREATE REL TABLE after (FROM V TO V);
#         """
#     )

#     # If wrapping everyting in a transaction, result is consistent.
#     # conn.execute("BEGIN TRANSACTION")

#     # Separate CREATE statements of nodes for error reproduction.
#     for id in range(1, 5):
#         conn.execute(
#             "CREATE (:V {id: $id});",
#             {"id": id},
#         )

#     # .. and afterwards doing creation of relations in a separate statement.
#     # (If doing everything in one CREATE statement, result is consistent.
#     # (But neither, if using multiple `CREATE` in one statement)
#     conn.execute(
#         """
#         MATCH (a:V {id: 1}), (b:V {id: 2}),(c:V {id: 3}),(d:V {id: 4})
#         CREATE (a)-[:after]->(c), (b)-[:after]->(d);
#         """,
#     )

#     # Mutating statement: move nodes :after other nodes.
#     # See visualization in https://github.com/kuzudb/kuzu/issues/2303
#     conn.execute(
#         """
#         MATCH (s:V {id: $moving_id})
#         OPTIONAL MATCH (a:V {id: $moved_after_id})
#         OPTIONAL MATCH (b0:V)-[a0:after]->(s)
#         OPTIONAL MATCH (s)-[a1:after]->(b1:V)
#         OPTIONAL MATCH (b2:V)-[a2:after]->(a)
#         DELETE a0,a1,a2
#         CREATE
#         (s)-[:after]->(a),
#         (b0)-[:after]->(b1),
#         (b2)-[:after]->(s);
#         """,
#         {"moving_id": 1, "moved_after_id": 2},
#     )

#     # conn.execute("COMMIT")

#     # This won't print anything, if above operations are not wrapped in
#     # BEGIN TRANSACTION ... COMMIT.
#     result = conn.execute(
#         """
#         MATCH (a:V {id: $id2})-[b:after]->(c:V {id: $id4})
#         RETURN a.id, label(b), c.id;
#         """,
#         {"id2": 2, "id4": 4},
#     )
#     assert isinstance(result, kuzu.QueryResult)
#     while result.has_next():
#         print(result.get_next())
