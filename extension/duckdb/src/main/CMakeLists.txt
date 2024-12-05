add_library(kuzu_duckdb_extension
        OBJECT
        duckdb_extension.cpp)

set(DUCKDB_EXTENSION_OBJECT_FILES
        ${DUCKDB_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_duckdb_extension>
        PARENT_SCOPE)
