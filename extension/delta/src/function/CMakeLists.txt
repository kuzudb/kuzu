add_library(kuzu_delta_function
        OBJECT
        delta_scan.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_result_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_type_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/function/duckdb_scan.cpp)

set(DELTA_EXTENSION_OBJECT_FILES
        ${DELTA_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_delta_function>
        PARENT_SCOPE)
