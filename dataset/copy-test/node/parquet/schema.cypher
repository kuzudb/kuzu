CREATE node table tableOfTypes (id INT64 PRIMARY KEY, int64Column INT64, doubleColumn DOUBLE, booleanColumn BOOLEAN, dateColumn DATE, stringColumn STRING, listOfInt64 INT64[], listOfString STRING[], listOfListOfInt64 INT64[][], structColumn STRUCT(ID int64, name STRING));
CREATE NODE TABLE person(ID UINT64 PRIMARY KEY);
