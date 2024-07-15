create node table person1 (ID INT64, PRIMARY KEY (ID));
create node table person2 (ID INT64, PRIMARY KEY (ID));
create rel table knows11 (FROM person1 TO person1)
create rel table knows12 (FROM person1 TO person2);
create rel table knows21 (FROM person2 TO person1);
create rel table knows22 (FROM person2 TO person2);