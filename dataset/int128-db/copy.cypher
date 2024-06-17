create node table person (ID INT128, PRIMARY KEY (ID));
PROFILE COPY person FROM "dataset/int128-db/vPerson.csv";
PROFILE MATCH (a:person) RETURN a.ID;
