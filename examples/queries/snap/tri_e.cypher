MATCH (a:ND)-[e1:links]->(b:ND)-[e2:links]->(c:ND), (a)-[e3:links]->(c) WHERE a.X < 100000000 RETURN MIN(a.X), MIN(b.X), MIN(c.X)
