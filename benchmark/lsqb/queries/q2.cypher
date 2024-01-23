MATCH 
  (person1:Person)-[:Person_knows_Person]->(person2:Person), 
  (person1)<-[:Comment_hasCreator_Person]-(comment:Comment)-[:Comment_replyOf_Post]->(Post:Post)-[:Post_hasCreator_Person]->(person2) 
RETURN count(*) AS count
