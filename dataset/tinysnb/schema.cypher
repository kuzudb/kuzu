create node table person (ID INt64, fName StRING, gender INT64, isStudent BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], grades INT64[4], height float, PRIMARY KEY (ID));
create node table organisation (ID INT64, name STRING, orgCode INT64, mark DOUBLE, score INT64, history STRING, licenseValidInterval INTERVAL, rating DOUBLE, PRIMARY KEY (ID));
create node table movies (name STRING, length INT32, note STRING, PRIMARY KEY (name));
create rel table knows (FROM person TO person, date DATE, meetTime TIMESTAMP, validInterval INTERVAL, comments STRING[], MANY_MANY);
create rel table studyAt (FROM person TO organisation, year INT64, places STRING[], length INT16,MANY_ONE);
create rel table workAt (FROM person TO organisation, year INT64, grading DOUBLE[2], rating float, MANY_ONE);
create rel table meets (FROM person TO person, location FLOAT[2], times INT, MANY_ONE);
create rel table marries (FROM person TO person, usedAddress STRING[], address INT16[2], note STRING, ONE_ONE);
