MATCH (:Country)<-[:City_isPartOf_Country]-(:City)<-[:Person_isLocatedIn_City]-(:Person)<-[:Forum_hasMember_Person]-(:Forum)-[:Forum_containerOf_Post]->(:Post)
RETURN count(*) AS count
