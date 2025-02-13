add_library(kuzu_unity_catalog_connector
        OBJECT
        unity_catalog_connector.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_connector.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_result_converter.cpp)

set(UNITY_CATALOG_EXTENSION_OBJECT_FILES
        ${UNITY_CATALOG_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_unity_catalog_connector>
        PARENT_SCOPE)
