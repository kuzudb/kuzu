include_directories(
        src/include
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        ${PROJECT_SOURCE_DIR}/third_party/yyjson/src
)

add_subdirectory(src/functions/cast_functions)
add_subdirectory(src/functions/creation_functions)
add_subdirectory(src/functions/export_functions)
add_subdirectory(src/functions/extract_functions)
add_subdirectory(src/functions/scalar_functions)
add_subdirectory(src/functions/table_functions)
add_subdirectory(src/utils)
add_subdirectory(src/reader)
add_subdirectory(src/type)
add_subdirectory(src/common)
add_subdirectory(src/main)

add_library(json_extension
        SHARED
        ${JSON_OBJECT_FILES}
)

set_extension_properties(json_extension json json)

target_link_libraries(json_extension
        PRIVATE
        yyjson
)

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(json_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(json_extension)
endif ()
