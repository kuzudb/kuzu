include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        src/include
        third_party/snowball/libstemmer)

add_subdirectory(src/function)
add_subdirectory(src/main)

build_extension_lib(${BUILD_STATIC_EXTENSION} "neo4j")
