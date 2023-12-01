create node table person (ID INT64, PRIMARY KEY (ID));
create rel table knows (FROM person TO person, int64Prop INT64, MANY_MANY);