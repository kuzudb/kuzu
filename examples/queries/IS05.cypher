MATCH (comment:Comment)−[:comment_hasCreator]->(p:Person)
WHERE comment.ID = 39582418599937
RETURN p.ID, p.firstName, p.lastName;