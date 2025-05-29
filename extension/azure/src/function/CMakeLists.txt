add_library(kuzu_azure_function
        OBJECT
        azure_scan.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_result_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_type_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/function/duckdb_scan.cpp)

set(AZURE_EXTENSION_OBJECT_FILES
        ${AZURE_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_azure_function>
        PARENT_SCOPE)
