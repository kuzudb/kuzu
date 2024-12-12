add_library(kuzu_sqlite_storage
        OBJECT
        sqlite_storage.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/catalog/duckdb_catalog.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/function/duckdb_scan.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/catalog/duckdb_table_catalog_entry.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/function/clear_cache.cpp)

set(SQLITE_EXTENSION_OBJECT_FILES
        ${SQLITE_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_sqlite_storage>
        PARENT_SCOPE)
