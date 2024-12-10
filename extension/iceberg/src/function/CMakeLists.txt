add_library(kuzu_iceberg_scan
        OBJECT
        iceberg_scan.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_result_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_type_converter.cpp)

set(ICEBERG_OBJECT_FILES
        ${ICEBERG_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_iceberg_scan>
        PARENT_SCOPE)
