if (WIN32)
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

add_library(kuzu_duckdb_extension
        SHARED
        ${DUCKDB_EXTENSION_OBJECT_FILES})

target_link_libraries(kuzu_duckdb_extension
        PRIVATE
        ${DuckDB_LIBRARIES})

set_extension_properties(kuzu_duckdb_extension duckdb duckdb)

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(kuzu_duckdb_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(kuzu_duckdb_extension)
endif ()
