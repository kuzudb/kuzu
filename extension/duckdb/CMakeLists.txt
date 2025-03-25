if (WIN32 OR BUILD_STATIC_EXTENSION)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()
find_package(DuckDB REQUIRED)

include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/include  # For S3 configuration
        src/include
        ${DuckDB_INCLUDE_DIRS})

add_subdirectory(src/catalog)
add_subdirectory(src/connector)
add_subdirectory(src/main)
add_subdirectory(src/function)
add_subdirectory(src/storage)
add_subdirectory(src/installer)
add_subdirectory(src/loader)

build_extension_lib(${BUILD_STATIC_EXTENSION} "duckdb")

target_link_libraries(kuzu_${EXTENSION_LIB_NAME}_extension
        PRIVATE
        ${DuckDB_LIBRARIES})
