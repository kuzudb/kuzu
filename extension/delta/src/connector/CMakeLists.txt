add_library(kuzu_delta_connector
        OBJECT
        delta_connector.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/duckdb_secret_manager.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_connector.cpp)

set(DELTA_OBJECT_FILES
        ${DELTA_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_delta_connector>
        PARENT_SCOPE)
