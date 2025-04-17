create node table person (ID INT16, gender INT32, PRIMARY KEY (ID));
create node table personString (ID INT16, str STRING, PRIMARY KEY (ID));
create node table movie (ID INT16, length INT32, name STRING, PRIMARY KEY (ID));
create node table Comment (id int64, creationDate INT64, locationIP STRING, browserUsed STRING, content STRING, length INT32, PRIMARY KEY (id));
create rel table watch (FROM person TO movie);
create rel table likes (FROM person TO movie, score INT32, comment STRING, MANY_MANY);
