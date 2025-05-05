include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        src/include)

add_subdirectory(src/main)
add_subdirectory(src/function)
add_subdirectory(src/common)
add_subdirectory(test)

build_extension_lib(${BUILD_STATIC_EXTENSION} "algo")
