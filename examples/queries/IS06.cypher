MATCH (comment:Comment)−[e1:replyOf_post]->(post:Post)<-[e2:containerOf]−(f:Forum)−[e3:hasModerator]->(p:Person)
WHERE comment.ID = 39582418599937
RETURN f.ID, f.title, p.ID, p.firstName, p.lastName;