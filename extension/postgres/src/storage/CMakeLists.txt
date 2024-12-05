add_library(kuzu_postgres_storage
        OBJECT
        postgres_storage.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/catalog/duckdb_catalog.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/function/duckdb_scan.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/catalog/duckdb_table_catalog_entry.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/function/clear_cache.cpp)

set(POSTGRES_EXTENSION_OBJECT_FILES
        ${POSTGRES_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_postgres_storage>
        PARENT_SCOPE)
