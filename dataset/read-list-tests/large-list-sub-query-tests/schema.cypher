create node table person (ID INT64, PRIMARY KEY (ID));
create rel table knows (FROM person TO person, MANY_MANY);