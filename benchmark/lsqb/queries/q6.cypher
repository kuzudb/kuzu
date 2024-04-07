MATCH (person1:Person)-[:Person_knows_Person]->(person2:Person)-[:Person_knows_Person]->(person3:Person)-[:Person_hasInterest_Tag]->(tag:Tag) 
WHERE id(person1) <> id(person3) 
RETURN count(*) AS count
