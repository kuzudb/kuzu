create node table person (ID int64, primary key(ID))
create rel table knows (FROM person TO person, weight INT64)
