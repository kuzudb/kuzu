create node table person (ID INT64, gender INT32, PRIMARY KEY (ID));
create node table movie (ID INT64, length INT32, PRIMARY KEY (ID));
create rel table watch (FROM person TO movie);
