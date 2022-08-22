MATCH (mAuth:Person)<-[e1:comment_hasCreator]−(cmt0:Comment)<-[e2:replyOf_comment]−(cmt1:Comment)−[e3:comment_hasCreator]->(rAuth:Person)
WHERE cmt0.ID = 65970697666569
RETURN cmt1.ID, cmt1.content, cmt1.creationDate, rAuth.ID, rAuth.firstName, rAuth.lastName;
