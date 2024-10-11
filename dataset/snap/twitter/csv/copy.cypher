COPY account FROM "dataset/snap/twitter/csv/twitter-nodes.csv";
COPY follows FROM "dataset/snap/twitter/csv/twitter-edges.csv" (DELIM=' ');
