add_library(kuzu_iceberg_connector
        OBJECT
        iceberg_connector.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_secret_manager.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_connector.cpp)

set(ICEBERG_OBJECT_FILES
        ${ICEBERG_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_iceberg_connector>
        PARENT_SCOPE)
