COPY account FROM "dataset/snap/amazon0601/csv/amazon-nodes.csv";
COPY follows FROM "dataset/snap/amazon0601/csv/amazon-edges.csv" (DELIM="\\t");
