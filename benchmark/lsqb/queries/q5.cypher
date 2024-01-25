MATCH (tag1:Tag)<-[:Post_hasTag_Tag|:Comment_hasTag_Tag]-(message:Comment:Post)<-[:Comment_replyOf_Post|:Comment_replyOf_Comment]-(comment:Comment)-[:Comment_hasTag_Tag]->(tag2:Tag) 
WHERE id(tag1) <> id(tag2) 
RETURN count(*) AS count
