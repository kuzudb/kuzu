include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        src/include
        third_party/simsimd/include)

add_subdirectory(src)
#add_subdirectory(test)

add_library(hnsw_extension
        SHARED
        ${HNSW_OBJECT_FILES})

add_subdirectory(third_party)

target_link_libraries(hnsw_extension
        PRIVATE
        simsimd)

set_extension_properties(hnsw_extension hnsw hnsw)

if (WIN32)
    # See comment in extension/httpfs/CMakeLists.txt
    target_link_libraries(hnsw_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(hnsw_extension)
endif ()
