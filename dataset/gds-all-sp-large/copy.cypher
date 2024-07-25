COPY person1 FROM "dataset/gds-all-sp-large/vPerson.csv"
COPY person2 FROM "dataset/gds-all-sp-large/vPerson.csv"
COPY knows11 FROM "dataset/gds-all-sp-large/vKnows.csv" (DELIM=" ")
COPY knows12 FROM "dataset/gds-all-sp-large/vKnows.csv" (DELIM=" ")
COPY knows21 FROM "dataset/gds-all-sp-large/vKnows.csv" (DELIM=" ")
