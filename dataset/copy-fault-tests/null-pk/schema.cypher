create node table person (fName STRING, PRIMARY KEY (fName));
create node table movie (ID INT64, length INT32, PRIMARY KEY (ID));
create rel table like (FROM person TO movie);
