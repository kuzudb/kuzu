CALL threads=2;
COPY serialtable (ID2) FROM ["dataset/large-serial/serialtable0.csv", "dataset/large-serial/serialtable1.csv", "dataset/large-serial/serialtable2.csv", "dataset/large-serial/serialtable3.csv", "dataset/large-serial/serialtable4.csv"];
