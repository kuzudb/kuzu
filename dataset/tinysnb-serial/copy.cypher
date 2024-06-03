COPY person (fName, gender, isStudent, isWorker, age, eyeSight, birthdate, registerTime, lastJobDuration, workedHours, usedNames, courseScoresPerTerm, grades, height) FROM "dataset/tinysnb-serial/vPerson.csv" (HeaDER=true, deLim=',');
COPY organisation (name, orgCode, mark, score, history, licenseValidInterval, rating) FROM "dataset/tinysnb-serial/vOrganisation.csv";
COPY movies (name, length, note) FROM "dataset/tinysnb-serial/vMovies.csv";
COPY knows FROM "dataset/tinysnb-serial/eKnows.csv";
COPY studyAt FROM "dataset/tinysnb-serial/eStudyAt.csv" (HEADeR=true);
COPY workAt FROM "dataset/tinysnb-serial/eWorkAt.csv"
COPY meets FROM "dataset/tinysnb-serial/eMeets.csv"
COPY marries FROM "dataset/tinysnb-serial/eMarries.csv"
