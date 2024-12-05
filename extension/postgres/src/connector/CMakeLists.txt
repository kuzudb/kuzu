add_library(kuzu_postgres_connector
        OBJECT
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_connector.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_result_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_type_converter.cpp
        postgres_connector.cpp)

set(POSTGRES_EXTENSION_OBJECT_FILES
        ${POSTGRES_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_postgres_connector>
        PARENT_SCOPE)
