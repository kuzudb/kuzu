CALL gds.graph.drop('GraphUndirected');
CALL gds.graph.project('GraphUndirected', 'Node', {Edge: {orientation: 'UNDIRECTED'}});
CALL gds.kcore.stream('GraphUndirected') YIELD coreValue RETURN coreValue, count(*) AS count ORDER BY count DESC LIMIT 10;
