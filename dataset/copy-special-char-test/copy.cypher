COPY person FROM "dataset/copy-special-char-test/vPerson.csv" (ESCAPE = "#", QUOTE = "-", DELIM="|");
COPY organisation FROM "dataset/copy-special-char-test/vOrganisation.csv" (ESCAPE = "#", QUOTE = "=", DELIM=",");
COPY workAt FROM "dataset/copy-special-char-test/eWorkAt.csv" (ESCAPE = "#", QUOTE = "=", DELIM="|");