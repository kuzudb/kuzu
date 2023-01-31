create node table animal (ID INT64, PRIMARY KEY (ID));
create node table person (ID INT64, PRIMARY KEY (ID));
create rel table knows (FROM person TO person, length INT64, place STRING, tag STRING[], MANY_MANY);
create rel table hasOwner (FROM animal TO person, length INT64, place STRING, ONE_ONE)
create rel table teaches (FROM person TO person, length INT64, MANY_ONE)