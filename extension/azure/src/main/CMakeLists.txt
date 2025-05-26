add_library(azure_extension_main
        OBJECT
        azure_extension.cpp
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/main/duckdb_options.cpp
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/s3fs_config.cpp)

set(AZURE_EXTENSION_OBJECT_FILES
        ${AZURE_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:azure_extension_main>
        PARENT_SCOPE)
