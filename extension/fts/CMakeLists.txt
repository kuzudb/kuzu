include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        src/include
        third_party/snowball/libstemmer)

add_subdirectory(src/catalog)
add_subdirectory(src/function)
add_subdirectory(src/main)
add_subdirectory(test)
add_subdirectory(src/index)
add_subdirectory(src/utils)

add_subdirectory(third_party/snowball)

build_extension_lib(${BUILD_STATIC_EXTENSION} "fts")

target_link_libraries(kuzu_${EXTENSION_LIB_NAME}_extension
        PRIVATE
        snowball
        re2)
