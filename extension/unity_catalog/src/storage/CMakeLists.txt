add_library(kuzu_unity_catalog_storage
        OBJECT
        unity_catalog_storage.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/catalog/duckdb_catalog.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/function/duckdb_scan.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/function/clear_cache.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/connector/duckdb_type_converter.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/catalog/duckdb_table_catalog_entry.cpp
)

set(UNITY_CATALOG_EXTENSION_OBJECT_FILES
        ${UNITY_CATALOG_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_unity_catalog_storage>
        PARENT_SCOPE)
