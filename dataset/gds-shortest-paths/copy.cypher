COPY person1 FROM "dataset/gds-shortest-paths/vPerson.csv"
COPY person2 FROM "dataset/gds-shortest-paths/vPerson.csv"
COPY knows11 FROM "dataset/gds-shortest-paths/vKnows.csv" (DELIM=" ")
COPY knows12 FROM "dataset/gds-shortest-paths/vKnows.csv" (DELIM=" ")
COPY knows21 FROM "dataset/gds-shortest-paths/vKnows.csv" (DELIM=" ")