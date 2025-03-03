CALL gds.graph.drop('GraphUndirected');
CALL gds.graph.project('GraphUndirected', 'Node', {Edge: {orientation: 'UNDIRECTED'}});
CALL gds.wcc.stream('GraphUndirected') YIELD nodeId, componentId RETURN componentId, MIN(gds.util.asNode(nodeId).id) as idmin, count(*) AS count, SUM(gds.util.asNode(nodeId).id) as idsum ORDER BY count DESC, idmin LIMIT 10;
