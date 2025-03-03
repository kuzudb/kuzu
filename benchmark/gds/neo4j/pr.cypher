CALL gds.graph.drop('GraphDirected');
CALL gds.graph.project('GraphDirected', 'Node', 'Edge');
CALL gds.pageRank.stream('GraphDirected') YIELD nodeId, score RETURN gds.util.asNode(nodeId).id AS id, score ORDER BY score DESC LIMIT 10
