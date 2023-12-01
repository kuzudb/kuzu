add_library(kuzu_processor_operator_parquet_reader
        OBJECT
        boolean_column_reader.cpp
        column_reader.cpp
        parquet_reader.cpp
        interval_column_reader.cpp
        struct_column_reader.cpp
        string_column_reader.cpp
        list_column_reader.cpp
        parquet_timestamp.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_parquet_reader>
        PARENT_SCOPE)
