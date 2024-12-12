add_library(kuzu_delta_extension
        OBJECT
        delta_extension.cpp
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/s3_download_options.cpp)

target_include_directories(
        kuzu_delta_extension
        PRIVATE
)

set(DELTA_EXTENSION_OBJECT_FILES
        ${DELTA_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_delta_extension>
        PARENT_SCOPE)
