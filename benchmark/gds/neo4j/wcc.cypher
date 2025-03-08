CALL gds.graph.drop('GraphUndirected');
CALL gds.graph.project('GraphUndirected', 'Node', {Edge: {orientation: 'UNDIRECTED'}});
CALL gds.wcc.stream('GraphUndirected') YIELD nodeId, componentId WITH componentId, MIN(gds.util.asNode(nodeId).id) as wccId, count(*) AS nodeCount, SUM(gds.util.asNode(nodeId).id) as nodeIdSum RETURN wccId, nodeCount, nodeIdSum ORDER BY nodeCount DESC, wccId LIMIT 10;
