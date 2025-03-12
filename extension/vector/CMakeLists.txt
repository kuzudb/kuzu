include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        src/include
        ${PROJECT_SOURCE_DIR}/third_party/simsimd/include)

add_subdirectory(src)

add_library(vector_extension
        SHARED
        ${VECTOR_OBJECT_FILES})

target_link_libraries(vector_extension
        PRIVATE
        simsimd)

set_extension_properties(vector_extension vector vector)

if (WIN32)
    # See comment in extension/httpfs/CMakeLists.txt
    target_link_libraries(vector_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(vector_extension)
endif ()
