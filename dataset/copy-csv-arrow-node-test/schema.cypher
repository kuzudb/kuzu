create node table arrow (id INT64, feature1 INT64, feature2 DOUBLE, feature3 BOOLEAN, feature4 DATE, feature5 TIMESTAMP, feature6 STRING, PRIMARY KEY (id));
create node table arrow_list (id INT64, feature1 INT64[], feature2 DOUBLE[], feature3 BOOLEAN[], feature4 DATE[], feature5 TIMESTAMP[], feature6 STRING[], PRIMARY KEY (id));
create node table arrow_csv (id INT64, feature1 INT64, feature2 DOUBLE, feature3 BOOLEAN, feature4 DATE, feature5 TIMESTAMP, feature6 STRING, feature7 INTERVAL, feature8 INT64[][], PRIMARY KEY (id));
create node table arrow_arrow (id INT64, feature1 INT64, feature2 DOUBLE, feature3 BOOLEAN, feature4 DATE, feature5 TIMESTAMP, feature6 STRING, PRIMARY KEY (id));
create node table arrow_parquet (id INT64, feature1 INT64, feature2 DOUBLE, feature3 BOOLEAN, feature4 DATE, feature5 TIMESTAMP, feature6 STRING, PRIMARY KEY (id));
