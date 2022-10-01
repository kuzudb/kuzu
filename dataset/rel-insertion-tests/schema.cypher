create node table animal (ID INT64, PRIMARY KEY (ID));
create node table person (ID INT64, PRIMARY KEY (ID));
create rel table knows (FROM animal|person TO animal|person, length INT64, place STRING, MANY_MANY);