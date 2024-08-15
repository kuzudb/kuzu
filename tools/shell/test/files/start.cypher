create node table person 
(ID INt64, fName StRING, gender INT64, isStudent BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, 

registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], 
grades INT64[4], height float, u UUID, PRIMARY KEY (ID));
create node table organisation (ID INT64, name STRING, orgCode INT64, mark DOUBLE, score INT64, history STRING, licenseValidInterval INTERVAL, rating DOUBLE, state STRUCT(revenue INT16, location STRING[], stock STRUCT(price INT64[], volume INT64)), info UNION(price FLOAT, movein DATE, note STRING),PRIMARY KEY (ID));
create node table movies (name STRING, length INT32, note STRING, 
description STRUCT(rating DOUBLE, stars INT8, views INT64, release TIMESTAMP, release_ns TIMESTAMP_NS, release_ms TIMESTAMP_MS, release_sec TIMESTAMP_SEC, release_tz TIMESTAMP_TZ, film DATE, u8 UINT8, u16 UINT16, u32 UINT32, u64 UINT64, hugedata INT128), content BYTEA, audience MAP(STRING, INT64), grade union(credit boolean, grade1 double, grade2 int64), PRIMARY KEY (name));
create rel table knows (FROM person TO person, date DATE, meetTime TIMESTAMP, validInterval INTERVAL, comments STRING[], summary STRUCT(locations STRING[], transfer STRUCT(day DATE, amount INT64[])), notes UNION(firstmet DATE, type INT16, comment STRING), someMap MAP(STRING, STRING), MANY_MAnY);
create rel table studyAt (FROM person TO organisation, year INT64, places STRING[], length INT16, level INT8, 
code UINT64, temperature UINT32, ulength UINT16, ulevel UINT8, hugedata INT128, MANY_ONe);
create rel table workAt (FROM person TO organisation, year INT64, grading DOUBLE[2], rating float, MANY_ONE);
create rel table meets (FROM person TO person, location FLOAT[2], times INT, data BYTEA, 
MANY_ONE); create rel table marries (FROM person TO person, usedAddress STRING[], address INT16[2], note STRING, ONE_ONE);
