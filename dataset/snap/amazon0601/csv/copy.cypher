COPY account FROM "dataset/snap/amazon0601/csv/amazon-nodes.csv" (HEADER=false);
COPY follows FROM "dataset/snap/amazon0601/csv/amazon-edges.csv" (HEADER=false, DELIM="\\t");
