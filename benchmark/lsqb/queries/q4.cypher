MATCH (:Tag)<-[:Post_hasTag_Tag|:Comment_hasTag_Tag]-(message:Post:Comment)-[:Post_hasCreator_Person|:Comment_hasCreator_Person]->(creator:Person), 
  (message)<-[:Person_likes_Comment|:Person_likes_Post]-(liker:Person), 
  (message)<-[:Comment_replyOf_Comment|:Comment_replyOf_Post]-(comment:Comment) 
RETURN count(*) AS count
