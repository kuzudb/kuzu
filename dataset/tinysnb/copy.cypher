COPY person FROM "dataset/tinysnb/vPerson.csv" (HEADER=true);
COPY organisation FROM "dataset/tinysnb/vOrganisation.csv";
COPY movies FROM "dataset/tinysnb/vMovies.csv";
COPY knows FROM "dataset/tinysnb/eKnows.csv";
COPY studyAt FROM "dataset/tinysnb/eStudyAt.csv" (HEADER=true);
COPY workAt FROM "dataset/tinysnb/eWorkAt.csv"
COPY meets FROM "dataset/tinysnb/eMeets.csv"
COPY marries FROM "dataset/tinysnb/eMarries.csv"
