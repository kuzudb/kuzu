include_directories(
        src/include
        ${PROJECT_SOURCE_DIR}/src/include
        third_party/yyjson/src
)

add_subdirectory(src)

add_library(kuzu_json_extension
        SHARED
        ${JSON_OBJECT_FILES}
)

add_subdirectory(third_party)

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
