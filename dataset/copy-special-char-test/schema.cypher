create node table person (ID INT64, randomString STRING, date DATE, PRIMARY KEY (ID));
create node table organisation (ID INT64, name STRING, orgCode INT64, mark DOUBLE,score INT64, history STRING, licenseValidInterval INTERVAL, PRIMARY KEY (ID));
create rel table workAt (FROM person TO organisation, length STRING, MANY_ONE);