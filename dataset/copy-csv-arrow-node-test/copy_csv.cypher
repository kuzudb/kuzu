COPY arrow FROM "dataset/copy-csv-arrow-node-test/types_10k.csv" (HEADER=true);
COPY arrow_list FROM "dataset/copy-csv-arrow-node-test/list.csv" (HEADER=true);
COPY arrow_csv FROM "dataset/copy-csv-arrow-node-test/types.csv";
COPY arrow_arrow FROM "dataset/copy-csv-arrow-node-test/types.arrow";
COPY arrow_parquet FROM "dataset/copy-csv-arrow-node-test/types.parquet";
