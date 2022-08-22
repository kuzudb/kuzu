MATCH (p:Person)-[e1:person_isLocatedIn]->(pl:Place) 
WHERE p.ID = 933 
RETURN p.firstName, p.lastName, p.birthday, p.locationIP, p.browserUsed, p.gender, p.creationDate, pl.ID;