include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        src/include
        third_party/snowball/libstemmer)

# Enable Jieba for all targets in this extension
add_compile_definitions(KUZU_ENABLE_JIEBA)

# Add cppjieba include directories for all targets in this extension
include_directories(
        ${PROJECT_SOURCE_DIR}/third_party/cppjieba/include
        ${PROJECT_SOURCE_DIR}/third_party/cppjieba/deps/limonp/include)

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
        re2
        cppjieba)

# Add cppjieba include directories
target_include_directories(kuzu_${EXTENSION_LIB_NAME}_extension PRIVATE
        ${PROJECT_SOURCE_DIR}/third_party/cppjieba/include
        ${PROJECT_SOURCE_DIR}/third_party/cppjieba/deps/limonp/include)

# Copy Jieba dictionaries next to the built extension so runtime can find a default path
add_custom_command(TARGET kuzu_${EXTENSION_LIB_NAME}_extension POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${PROJECT_SOURCE_DIR}/third_party/cppjieba/dict
        ${PROJECT_SOURCE_DIR}/extension/fts/build/dict)
