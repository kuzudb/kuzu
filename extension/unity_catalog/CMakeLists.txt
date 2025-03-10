if (WIN32)
    set(DuckDB_USE_STATIC_LIBS ON)
else ()
    set(DuckDB_USE_STATIC_LIBS OFF)
endif ()
find_package(DuckDB REQUIRED)

include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        src/include
        ${PROJECT_SOURCE_DIR}/extension/duckdb/src/include
        ${PROJECT_SOURCE_DIR}/extension/httpfs/src/include  # For S3 configuration
        ${DuckDB_INCLUDE_DIRS})

add_subdirectory(src/installer)
add_subdirectory(src/main)
add_subdirectory(src/connector)
add_subdirectory(src/storage)
add_subdirectory(src/options)

add_library(unity_catalog_extension
        SHARED
        ${UNITY_CATALOG_EXTENSION_OBJECT_FILES})

set_extension_properties(unity_catalog_extension unity_catalog unity_catalog)

target_link_libraries(unity_catalog_extension
        PRIVATE
        ${DuckDB_LIBRARIES})

if (NOT WIN32)
    add_library(unity_catalog_loader
            SHARED
            ${PROJECT_SOURCE_DIR}/extension/duckdb/src/loader/duckdb_loader.cpp)
    set_extension_properties(unity_catalog_loader unity_catalog_loader unity_catalog)
endif ()

if (WIN32)
    # See comments in extension/httpfs/CMakeLists.txt.
    target_link_libraries(unity_catalog_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(unity_catalog_extension)
    set_apple_dynamic_lookup(unity_catalog_loader)
endif ()
