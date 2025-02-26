include_directories(
        src/include
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        ${PROJECT_SOURCE_DIR}/third_party/yyjson/src
)

add_subdirectory(src)

add_library(kuzu_json_extension
        SHARED
        ${JSON_OBJECT_FILES}
)

set_extension_properties(kuzu_json_extension json json)

target_link_libraries(kuzu_json_extension
        PRIVATE
        yyjson
)

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(kuzu_json_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(kuzu_json_extension)
endif ()
