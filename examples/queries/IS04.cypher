MATCH (comment:Comment)
WHERE comment.ID = 39582418599937
RETURN comment.creationDate, comment.content;
