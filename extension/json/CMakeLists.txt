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

build_extension_lib(${BUILD_STATIC_EXTENSION} "json")

target_link_libraries(kuzu_${EXTENSION_LIB_NAME}_extension
        PRIVATE
        yyjson
)
