COPY person FROM "dataset/copy-special-char-test/vPerson.csv" (ESCAPE = "#", QUOTE = "-");
COPY organisation FROM "dataset/copy-special-char-test/vOrganisation.csv" (ESCAPE = "#", QUOTE = "=");
COPY workAt FROM "dataset/copy-special-char-test/eWorkAt.csv" (ESCAPE = "#", QUOTE = "=");
