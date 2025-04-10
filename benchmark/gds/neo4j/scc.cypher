CALL gds.graph.drop('GraphDirected');
CALL gds.graph.project('GraphDirected', 'Node', 'Edge');
CALL gds.scc.stream('GraphDirected') YIELD nodeId, componentId WITH componentId, MIN(gds.util.asNode(nodeId).id) as sccId, count(*) AS nodeCount, SUM(gds.util.asNode(nodeId).id) as nodeIdSum RETURN sccId, nodeCount, nodeIdSum ORDER BY nodeCount DESC, sccId LIMIT 10;
