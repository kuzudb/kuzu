COPY arrow_csv FROM "dataset/copy-arrow-node-test/types_50k.csv" (HEADER=true);
COPY arrow_arrow FROM "dataset/copy-arrow-node-test/types_50k.arrow" (HEADER=true);
COPY arrow_parquet FROM "dataset/copy-arrow-node-test/types_50k.parquet" (HEADER=true);
