add_library(kuzu_iceberg_main
        OBJECT
        iceberg_extension.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/main/duckdb_options.cpp
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/s3fs_config.cpp)

set(ICEBERG_EXTENSION_OBJECT_FILES
        ${ICEBERG_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_iceberg_main>
        PARENT_SCOPE)
