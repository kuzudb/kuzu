add_library(kuzu_duckdb_function
        OBJECT
        clear_cache.cpp
        duckdb_scan.cpp)

set(DUCKDB_EXTENSION_OBJECT_FILES
        ${DUCKDB_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_duckdb_function>
        PARENT_SCOPE)
