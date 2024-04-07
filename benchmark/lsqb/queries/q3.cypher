MATCH (country:Country) 
MATCH (person1:Person)-[:Person_isLocatedIn_City]->(city1:City)-[:City_isPartOf_Country]->(country) 
MATCH (person2:Person)-[:Person_isLocatedIn_City]->(city2:City)-[:City_isPartOf_Country]->(country) 
MATCH (person3:Person)-[:Person_isLocatedIn_City]->(city3:City)-[:City_isPartOf_Country]->(country) 
MATCH (person1)-[:Person_knows_Person]->(person2)-[:Person_knows_Person]->(person3)-[:Person_knows_Person]->(person1) 
RETURN count(*) AS count
