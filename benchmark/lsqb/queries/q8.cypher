MATCH (tag1:Tag)<-[:Post_hasTag_Tag|:Comment_hasTag_Tag]-(message:Post:Comment)<-[:Comment_replyOf_Post|:Comment_replyOf_Comment]-(comment:Comment)-[:Comment_hasTag_Tag]->(tag2:Tag) 
WHERE NOT EXISTS {MATCH (comment)-[:Comment_hasTag_Tag]->(tag1)} 
  AND id(tag1) <> id(tag2) 
RETURN count(*) AS count
