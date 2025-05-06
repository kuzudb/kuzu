include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        src/include
        ${PROJECT_SOURCE_DIR}/third_party/simsimd/include)

add_subdirectory(src/catalog)
add_subdirectory(src/function)
add_subdirectory(src/index)
add_subdirectory(src/main)
add_subdirectory(test)

build_extension_lib(${BUILD_STATIC_EXTENSION} "vector")

target_link_libraries(kuzu_${EXTENSION_LIB_NAME}_extension
        PRIVATE
        simsimd)
