add_library(kuzu_iceberg_functions
        OBJECT
        iceberg_bindfunc.cpp
        iceberg_scan.cpp
        iceberg_metadata.cpp
        iceberg_snapshots.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_type_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_result_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/delta/src/function/delta_scan.cpp
)

set(ICEBERG_EXTENSION_OBJECT_FILES
        ${ICEBERG_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_iceberg_functions>
        PARENT_SCOPE)
