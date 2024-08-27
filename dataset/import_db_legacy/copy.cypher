COPY person FROM "person.csv" (escape ='\\', delim =',', quote='\"', header=false);
COPY organisation FROM "organisation.csv" (escape ='\\', delim =',', quote='\"', header=false);
COPY studyAt FROM "studyAt.csv" (escape ='\\', delim =',', quote='\"', header=false);
