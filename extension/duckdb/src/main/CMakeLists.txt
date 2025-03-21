add_library(kuzu_duckdb_extension
        OBJECT
        duckdb_extension.cpp
        duckdb_options.cpp
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/s3fs_config.cpp)

set(DUCKDB_EXTENSION_OBJECT_FILES
        ${DUCKDB_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_duckdb_extension>
        PARENT_SCOPE)
