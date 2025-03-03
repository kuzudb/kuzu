CALL gds.graph.drop('GraphDirected');
CALL gds.graph.project('GraphDirected', 'Node', 'Edge');
CALL gds.scc.stream('GraphDirected') YIELD nodeId, componentId RETURN componentId, MIN(gds.util.asNode(nodeId).id) as idmin, count(*) AS count, SUM(gds.util.asNode(nodeId).id) as idsum ORDER BY count DESC, idmin LIMIT 10;
