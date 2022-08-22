MATCH (p:Person)−[e:knows]->(friend:Person)
WHERE p.ID = 933
RETURN friend.ID, friend.firstName, friend.lastName, e.creationDate;
