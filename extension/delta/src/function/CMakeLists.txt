add_library(kuzu_delta_scan
        OBJECT
        delta_scan.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_result_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_type_converter.cpp)

set(DELTA_OBJECT_FILES
        ${DELTA_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_delta_scan>
        PARENT_SCOPE)
