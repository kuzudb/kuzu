COPY person1 FROM "dataset/gds-shortest-paths-large/vPerson.csv"
COPY person2 FROM "dataset/gds-shortest-paths-large/vPerson.csv"
COPY knows11 FROM "dataset/gds-shortest-paths-large/vKnows.csv" (DELIM=" ")
COPY knows12 FROM "dataset/gds-shortest-paths-large/vKnows.csv" (DELIM=" ")
COPY knows21 FROM "dataset/gds-shortest-paths-large/vKnows.csv" (DELIM=" ")
