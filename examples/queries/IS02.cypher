MATCH (p:Person)<-[e1:comment_hasCreator]-(c:Comment)-[e2:replyOf_post]->(post:Post)-[e3:post_hasCreator]->(op:Person)
WHERE p.ID = 933
RETURN c.ID, c.content, c.creationDate, op.ID, op.firstName, op.lastName