add_library(delta_extension_main
        OBJECT
        delta_extension.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/main/duckdb_options.cpp
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/s3fs_config.cpp)

set(DELTA_EXTENSION_OBJECT_FILES
        ${DELTA_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:delta_extension_main>
        PARENT_SCOPE)
