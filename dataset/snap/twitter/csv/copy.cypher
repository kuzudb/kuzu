COPY account FROM "dataset/snap/twitter/csv/twitter-nodes.csv" (HEADER=false);
COPY follows FROM "dataset/snap/twitter/csv/twitter-edges.csv" (HEADER=false, DELIM=' ');
